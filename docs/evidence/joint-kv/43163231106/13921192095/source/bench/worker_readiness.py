"""Validate current Worker registration, runtime availability and placement capacity."""
from bench.substrate_live import timestamp


def current_workers(samples, now, max_age=5):
    """Use one completed fleet observation; retired and partial observations stay out."""
    marker = next((s for s in reversed(samples) if 'worker_inventory' in s), None)
    if marker is None or now - timestamp(marker['at']) > max_age:
        return {}
    members = set(marker['worker_inventory'])
    return {s['worker_name']: s for s in samples
            if s.get('worker_name') in members and s.get('observation') == marker['observation']
            and not s.get('error') and now - timestamp(s['at']) <= max_age}


def placement_capacity(sample, config):
    """Return total/free slots only after the reported gVisor runtime can place Actors."""
    status = sample.get('worker_status', {})
    if status.get('state') != 'WORKER_STATE_ACTIVE' or int(status.get('registeredEpoch', 0)) <= 0:
        return None
    classes = status.get('sandboxClasses', [])
    if len(classes) != 1 or classes[0].get('sandboxClass') != 'gvisor':
        return None
    runtime = classes[0].get('defaultRuntime', {})
    if not runtime.get('version', {}).get('schemaVersion'):
        return None
    overhead = runtime.get('actorOverhead', {})
    memory = config['actor_memory_bytes']
    booked = memory + int(overhead.get('memoryBytes', 0)) + memory * int(overhead.get('memoryBytesPerGib', 0)) // 2**30
    capacity, allocated = status.get('capacity', {}), status.get('allocated', {})
    dimensions = {'actors': 1, 'memoryBytes': booked, 'cpuMillicores': config['actor_cpu_millicores']}
    total = min(int(capacity.get(k, 0)) // size for k, size in dimensions.items())
    available = min(max(0, int(capacity.get(k, 0)) - int(allocated.get(k, 0))) // size
                    for k, size in dimensions.items())
    return {'total_slots': total, 'available_slots': available, 'actor_booked_memory_bytes': booked}
