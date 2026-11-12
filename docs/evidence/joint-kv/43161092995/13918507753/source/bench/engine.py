"""CPU discrete-event model with explicit actor and inference capacity queues."""
from __future__ import annotations
from dataclasses import dataclass, field
import heapq
import itertools


@dataclass
class Session:
    spec: dict
    turn: int = 0
    state: str = 'NEW'
    llm_phase: str = 'NEW'
    resident: bool = False
    done: bool = False
    cleanup_pending: bool = False
    epoch: int = 1
    generation: int = 0
    request_started: float = 0
    first_ttft: float | None = None
    model_started: float | None = None
    feedback_observed_at: float = 0.
    result_pending: bool = False
    snapshot_pending: bool = False
    resumes: list[float] = field(default_factory=list)
    completed_turns: int = 0


class Engine:
    def __init__(self, config: dict, group: dict, mode: str, ttl: float):
        self.config, self.group, self.mode, self.ttl = config, group, mode, ttl
        self.now = 0.
        self.counter = itertools.count()
        self.events = []
        self.raw = []
        self.sessions = {}
        self.actor_queue = []
        self.model_queue = []
        self.model_active = set()
        self.integrals = dict(actor_seconds=0., actor_gb_seconds=0., reserved_core_seconds=0.,
            blocked_actor_seconds=0., blocked_actor_gb_seconds=0., external_harness_gb_seconds=0.,
            estimated_inactive_token_seconds=0.)
        self.outcomes = []
        self.last_resource_completion = 0.
        self.admission_wait = []
        self.actor_wait = []

    def schedule(self, when, kind, session, value=None):
        heapq.heappush(self.events, (when, next(self.counter), kind, session, value))

    def record(self, event, session, **kwargs):
        self.raw.append(dict(at=self.now, event=event, session_id=session.spec['session_id'],
            turn=session.turn, epoch=session.epoch, **kwargs))

    def integrate(self, until):
        dt = until - self.now
        resident = [s for s in self.sessions.values() if s.resident and (not s.done or s.cleanup_pending)]
        blocked = [s for s in resident if s.state in ('MODEL_QUEUED', 'MODEL_RUNNING')]
        for key, value in [('actor_seconds',len(resident)),
            ('actor_gb_seconds',sum(s.spec['memory_gb'] for s in resident)),
            ('reserved_core_seconds',sum(s.spec['cores'] for s in resident)),
            ('blocked_actor_seconds',len(blocked)),
            ('blocked_actor_gb_seconds',sum(s.spec['memory_gb'] for s in blocked)),
            ('external_harness_gb_seconds',self.config['harness_memory_gb'] * sum((not s.done or s.cleanup_pending) and s.state != 'NEW' for s in self.sessions.values()) if self.mode == 'external' else 0),
            ('estimated_inactive_token_seconds',sum(s.spec.get('context_tokens',0) for s in self.sessions.values() if not s.done and s.llm_phase=='ACTING'))]:
            self.integrals[key] += dt * value
        self.now = until

    def actors_available(self):
        return self.config['actor_slots'] - sum(s.resident and (not s.done or s.cleanup_pending) for s in self.sessions.values())

    def require_actor(self, session, next_action):
        session.generation += 1
        if session.resident:
            self.schedule(self.now, next_action, session)
        elif self.actors_available() > 0:
            self.allocate(session, next_action, self.now)
        else:
            session.state = 'ACTOR_QUEUED'
            self.actor_queue.append((self.now, next(self.counter), session, next_action))
            self.record('actor_queued', session, next_action=next_action)

    def allocate(self, session, next_action, queued_at):
        session.resident = True
        session.state = 'RESUMING'
        delay = self.config['resume_seconds'] if session.epoch > 1 else 0
        session.resumes.append(delay)
        self.actor_wait.append(self.now - queued_at)
        self.record('actor_allocated', session, wait=self.now-queued_at, resume_seconds=delay)
        self.schedule(self.now + delay, next_action, session)

    def drain_actors(self):
        while self.actors_available() > 0 and self.actor_queue:
            queued_at, order, session, action = self.actor_queue.pop(0)
            if session.done:
                continue
            self.allocate(session, action, queued_at)

    def idle_allowed(self, session):
        return self.group['idle_suspend'] and (session.state == 'THINK' or self.mode == 'external')

    def maybe_suspend(self, session, gap):
        coordinate = self.group['wait_coordination'] and session.state in ('MODEL_QUEUED','MODEL_RUNNING')
        threshold = self.config['snapshot_seconds'] + self.config['resume_seconds']
        if coordinate and gap > threshold:
            delay = 0
        elif self.idle_allowed(session) and gap > self.ttl + threshold:
            delay = self.ttl
        else:
            return
        self.schedule(self.now+delay, 'suspend', session, session.generation)

    def acting_budget(self, session):
        if session.done or session.llm_phase != 'ACTING':
            return 0.
        weight=self.config['acting_token_weight']
        eligible=session.state in ('TOOL_RUNNING','THINK','SUSPENDING','SUSPENDED')
        if self.group.get('logical_state_feedback') and eligible and self.now-session.feedback_observed_at<=self.config['feedback_ttl_seconds']:
            weight=self.config['feedback_acting_weight']
        return weight*session.spec.get('context_tokens',0)

    def logical_load(self):
        active=sum(self.sessions[sid].spec.get('context_tokens',0) for sid in self.model_active)
        if not self.group['logical_admission']:
            return active
        # Inference program phases remain independent of Actor residency.
        return active+sum(self.acting_budget(s) for s in self.sessions.values())

    def drain_models(self):
        self.model_queue.sort(key=lambda x: (0 if x[2].spec['priority']=='interactive' else 1, x[0], x[1]))
        while self.model_queue and len(self.model_active) < self.config['model_slots']:
            at, _, session = self.model_queue[0]
            if session.done or session.state not in ('MODEL_QUEUED','SUSPENDING','SUSPENDED'):
                self.model_queue.pop(0)
                continue
            # Bounded timeout keeps low-priority sessions in the completion denominator.
            if self.group['logical_admission'] and self.logical_load() - self.acting_budget(session) + session.spec.get('context_tokens',0) > self.config['logical_token_capacity'] and self.now-at < 2-1e-9:
                self.schedule(max(self.now+.001,at+2), 'pump', session)
                break
            self.model_queue.pop(0)
            self.model_active.add(session.spec['session_id'])
            session.state = 'MODEL_RUNNING'
            session.llm_phase = 'REASONING'
            session.model_started = self.now
            self.admission_wait.append(self.now-at)
            self.record('model_started', session, logical_request_id=session.spec['rounds'][session.turn]['logical_request_id'], admission_wait=self.now-at,
                logical_load=self.logical_load(), logical_token_capacity=self.config['logical_token_capacity'], logical_feedback=self.group.get('logical_state_feedback',False))
            self.schedule(self.now+session.spec['rounds'][session.turn]['model_seconds'], 'model_done', session)

    def finish(self, session, status):
        if session.done:
            return
        session.cleanup_pending = status=='DEADLINE_EXCEEDED' and session.state=='TOOL_RUNNING'
        session.done = True
        session.llm_phase = 'FINAL'
        if not session.cleanup_pending:
            session.resident = False
        self.model_active.discard(session.spec['session_id'])
        latency = self.now-session.spec['arrival']
        outcome = dict(session_id=session.spec['session_id'], priority=session.spec['priority'], status=status,
            latency_seconds=latency, first_response_seconds=session.first_ttft,
            completed_turns=session.completed_turns, offered_turns=len(session.spec['rounds']),
            slo_success=status=='COMPLETED' and session.first_ttft is not None and session.first_ttft <= self.config['slo_seconds'])
        self.outcomes.append(outcome)
        self.record('session_terminal', session, cleanup_required=session.cleanup_pending, **{k:v for k,v in outcome.items() if k not in ('session_id',)})
        self.drain_actors()
        self.drain_models()

    def event(self, kind, session, value):
        if session.done:
            if kind=='tool_done' and session.cleanup_pending:
                session.cleanup_pending=False
                session.resident=False
                self.last_resource_completion=max(self.last_resource_completion,self.now)
                self.record('tool_cleanup_completed',session)
                self.drain_actors()
            return
        if kind == 'arrival':
            session.request_started = self.now
            self.record('arrival',session)
            self.require_actor(session,'model_request')
        elif kind == 'deadline':
            self.finish(session,'DEADLINE_EXCEEDED')
        elif kind == 'model_request':
            session.state = 'MODEL_QUEUED'
            session.generation += 1
            self.model_queue.append((self.now,next(self.counter),session))
            self.record('model_registered',session,logical_request_id=session.spec['rounds'][session.turn]['logical_request_id'])
            self.maybe_suspend(session,session.spec['rounds'][session.turn]['model_seconds'])
            self.drain_models()
        elif kind == 'model_done':
            self.model_active.discard(session.spec['session_id'])
            session.llm_phase = 'ACTING'
            if session.first_ttft is None:
                session.first_ttft = self.now-session.spec['arrival']
            self.record('model_completed',session)
            # Tool work waits for any admitted snapshot to become durable.
            if session.snapshot_pending:
                session.result_pending = True
            else:
                self.require_actor(session,'tool_start')
            self.drain_models()
        elif kind == 'tool_start':
            session.state = 'TOOL_RUNNING'
            session.feedback_observed_at = self.now
            session.generation += 1
            self.record('tool_started',session,tool_op_id=session.spec['rounds'][session.turn]['tool_op_id'])
            self.schedule(self.now+session.spec['rounds'][session.turn]['tool_seconds'],'tool_done',session)
        elif kind == 'tool_done':
            current = session.spec['rounds'][session.turn]
            if current['tool_failure']:
                self.finish(session,'TOOL_FAILED')
                return
            session.completed_turns += 1
            self.record('tool_completed',session,receipt_watermark=session.completed_turns)
            if session.turn+1 == len(session.spec['rounds']):
                self.finish(session,'COMPLETED')
            else:
                session.state = 'THINK'
                session.feedback_observed_at = self.now
                session.generation += 1
                self.maybe_suspend(session,current['think_seconds'])
                self.schedule(self.now+current['think_seconds'],'next_turn',session)
                self.drain_models()
        elif kind == 'next_turn':
            session.turn += 1
            session.request_started = self.now
            if session.snapshot_pending:
                session.result_pending = 'model_request'
            else:
                self.require_actor(session,'model_request')
        elif kind == 'suspend':
            if value != session.generation or not session.resident or session.state == 'TOOL_RUNNING':
                return
            session.state = 'SUSPENDING'
            session.feedback_observed_at = self.now
            session.snapshot_pending = True
            session.generation += 1
            self.record('safe_point',session,receipt_watermark=session.completed_turns)
            self.schedule(self.now+self.config['snapshot_seconds'],'suspended',session)
        elif kind == 'suspended':
            session.resident = False
            session.snapshot_pending = False
            session.state = 'SUSPENDED'
            session.epoch += 1
            self.record('suspended',session)
            self.drain_actors()
            if session.result_pending:
                action = 'model_request' if session.result_pending == 'model_request' else 'tool_start'
                session.result_pending = False
                self.require_actor(session,action)
        elif kind == 'pump':
            self.drain_models()

    def run(self, trace):
        for spec in trace['sessions']:
            session = Session(spec)
            self.sessions[spec['session_id']] = session
            self.schedule(spec['arrival'],'arrival',session)
            self.schedule(spec['arrival']+spec['deadline_seconds'],'deadline',session)
        while self.events:
            when, _, kind, session, value = heapq.heappop(self.events)
            self.integrate(when)
            self.event(kind,session,value)
        successes = [o for o in self.outcomes if o['status']=='COMPLETED']
        all_resumes = [v for s in self.sessions.values() for v in s.resumes if v]
        duration = max((o['latency_seconds']+self.sessions[o['session_id']].spec['arrival'] for o in self.outcomes),default=0)
        duration=max(duration,self.last_resource_completion)
        failures = len(self.outcomes)-len(successes)
        good = sum(o['slo_success'] for o in self.outcomes)
        interactive = [o for o in self.outcomes if o['priority']=='interactive']
        return dict(execution_kind='discrete-event-simulation', mode=self.mode, offered_count=len(trace['sessions']),
            completed_count=len(successes), failed_count=failures, incomplete_count=len(trace['sessions'])-len(self.outcomes),
            failure_rate=failures/len(trace['sessions']), goodput_per_second=good/duration if duration else 0,
            interactive_slo_rate=sum(o['slo_success'] for o in interactive)/len(interactive) if interactive else None,
            duration_seconds=duration, integrals=self.integrals, admission_wait_seconds=self.admission_wait,
            actor_wait_seconds=self.actor_wait, resume_seconds=all_resumes, outcomes=self.outcomes, events=self.raw,
            physical_kv={'status':'N/A','reason':'此运行仅模拟逻辑预算，物理KV动作与GPU遥测未配置'},
            logical_feedback={'enabled':self.group.get('logical_state_feedback',False),'target':'simulation inference-program Acting admission budget independent of Actor residency',
                'attribution':'E-D estimates logical-state feedback inside this explicit CPU model'},
            component_layers={'inference':'synthetic service-time model','actor':'capacity and lifecycle model','tools':'explicit duration/failure profile','native_router':'separate integration evidence'})
