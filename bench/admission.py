"""FIFO placement permits with observations recorded at the gate itself."""
from collections import Counter
import threading
import time
import uuid


class FairSlots:
    def __init__(self, count, stop=None, observe=None):
        if count < 1:
            raise ValueError('positive slot capacity required')
        self.stop, self.capacity = stop, count
        self.active = 0
        self.queue = []
        self.owners = {}
        self.cv = threading.Condition()
        self.observe = observe
        self.capture_failed = False

    def emit(self, kind, item, reason=''):
        if not self.observe:
            return
        now = time.monotonic_ns()
        try:
            self.observe('admission.'+kind, queue_episode_id=item['episode'],
                assignment_id=item['assignment'], session_id=item['name'],
                tenant_id=item['tenant'], actor_epoch=item['epoch'], queue_kind=item['kind'],
                requested_monotonic_ns=str(item['begin']),
                queue_age_ns=str(now-item['begin']), capacity=self.capacity,
                active=self.active, waiting=len(self.queue), reason=reason,
                tenant_backlog=dict(Counter(row['tenant'] for row in self.queue)),
                oldest_wait_ns=str(max((now-row['begin'] for row in self.queue), default=0)))
        except BaseException:
            self.capture_failed = True
            if self.stop: self.stop.set()
            self.cv.notify_all()
            raise

    def acquire(self, name, timeout, *, kind='resume_actor_admission', tenant='', epoch='unassigned'):
        item = dict(name=name, kind=kind, tenant=tenant, epoch=str(epoch),
            episode=uuid.uuid4().hex, assignment=uuid.uuid4().hex, begin=time.monotonic_ns())
        with self.cv:
            if name in self.owners or any(row['name']==name for row in self.queue):
                raise RuntimeError('session already owns or awaits a permit')
            if self.capture_failed: raise RuntimeError('admission capture failed')
            self.emit('requested', item)
            self.queue.append(item)
            try:
                self.emit('enqueued', item)
                while True:
                    remaining = timeout-(time.monotonic_ns()-item['begin'])/1e9
                    if remaining <= 0 or self.stop and self.stop.is_set():
                        self.queue.remove(item)
                        self.emit('cancelled' if self.stop and self.stop.is_set() else 'timeout', item,
                            'trial_stop' if self.stop and self.stop.is_set() else 'deadline')
                        self.cv.notify_all()
                        raise TimeoutError('Actor capacity queue deadline: '+name)
                    if self.queue[0] is item and self.active < self.capacity:
                        self.queue.pop(0)
                        self.active += 1
                        self.owners[name] = item
                        try:
                            self.emit('granted', item)
                        except BaseException:
                            del self.owners[name]
                            self.active -= 1
                            raise
                        self.cv.notify_all()
                        return (time.monotonic_ns()-item['begin'])/1e9
                    self.cv.wait(min(remaining, 1))
            except BaseException:
                if item in self.queue:
                    self.queue.remove(item)
                self.cv.notify_all()
                raise

    def release(self, name=None):
        with self.cv:
            if name is None:
                if len(self.owners)!=1:
                    raise RuntimeError('release requires a unique permit owner')
                name = next(iter(self.owners))
            if name not in self.owners:
                raise RuntimeError('slot released without ownership')
            item = self.owners.pop(name)
            self.active -= 1
            try:
                self.emit('released', item)
            finally:
                self.cv.notify_all()
