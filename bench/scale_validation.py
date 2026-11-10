"""Acceptance of recorded server decisions, including runs that always KEEP."""


def autonomous_evidence(states):
    decisions = [d for s in states for d in s.get('scheduling', {}).values()]
    observed = []
    for session in states:
        state = session.get('autonomous_scheduler') or {}
        evidence = state.get('evidence') or {}
        capacity = evidence.get('worker_capacity') or {}
        evaluated = state.get('last_evaluated_at', '')
        if (evaluated and not evaluated.startswith('0001-') and
                'coordinator_durable_request_state' in evidence.get('sources', []) and
                capacity.get('observed_at') and capacity.get('source')):
            observed.append(state)
    all_observed = all(d.get('estimate_source') == 'server_observed' and d.get('server_evidence') for d in decisions)
    return dict(total=len(decisions), suspend=sum(d['decision']['action'] == 'SUSPEND' for d in decisions),
                all_server_observed=bool(all_observed), evaluated_sessions=len(observed),
                keep_sessions=sum(s.get('last_action') == 'KEEP' for s in observed),
                server_observation_complete=bool(states) and len(observed) == len(states) and bool(all_observed))


def record_failure(report, error):
    """Retain the first failure while keeping each subsequent acceptance error."""
    report['status'] = 'failed'
    report.setdefault('error', error)
    report.setdefault('validation_errors', []).append(error)


def validate_program_protocol(states, expected_sessions):
    """Validate one terminal FINAL per E-feedback workspace, using global counters."""
    snapshots = [s.get('dynamo_finalized_snapshot') or {} for s in states]
    final_events = max((s.get('final_events', 0) for s in snapshots), default=0)
    return dict(expected_final_events=expected_sessions, final_events=final_events,
                symmetric_program_complete=len(states) == expected_sessions and
                all(s.get('ablation_group') == 'E' and s.get('dynamo_finalized') for s in states) and
                final_events == expected_sessions)
