"""
Queue Manager — handles task storage using Redis sorted sets (priority queue)
Falls back to in-memory sorted list when Redis is unavailable.
"""
import redis
import json
import uuid

# Connect to Redis
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()
    REDIS_AVAILABLE = True
except Exception:
    r = None
    REDIS_AVAILABLE = False

# In-memory fallback (sorted by priority score descending)
_memory_queue = []
_task_status = {}


def add_task(task_data, priority=1):
    """
    Add a task to the queue.
    Priority: higher number = higher priority (Redis sorted sets, zpopmax).
    Uses Redis zadd if available, else in-memory list.
    """
    if not task_data.get("task_id"):
        task_data["task_id"] = str(uuid.uuid4())[:8]

    serialized = json.dumps(task_data)

    if REDIS_AVAILABLE:
        r.zadd("task_queue", {serialized: priority})
    else:
        _memory_queue.append((priority, serialized))
        _memory_queue.sort(key=lambda x: x[0], reverse=True)

    save_status(task_data["task_id"], "queued")
    return task_data["task_id"]


def get_task():
    """
    Pop the highest-priority task from the queue.
    Returns the task dict or None if queue is empty.
    """
    if REDIS_AVAILABLE:
        result = r.zpopmax("task_queue")
        if result:
            return json.loads(result[0][0])
        return None
    else:
        if _memory_queue:
            _, serialized = _memory_queue.pop(0)
            return json.loads(serialized)
        return None


def queue_length():
    """Return current number of tasks waiting in the queue."""
    if REDIS_AVAILABLE:
        return r.zcard("task_queue")
    return len(_memory_queue)


def save_status(task_id, status):
    """Persist task status so it can be polled by the client."""
    if REDIS_AVAILABLE:
        r.hset("task_status", task_id, status)
    else:
        _task_status[task_id] = status


def get_status(task_id):
    """Retrieve the current status of a task by ID."""
    if REDIS_AVAILABLE:
        return r.hget("task_status", task_id)
    return _task_status.get(task_id, "not_found")


def all_statuses():
    """Return all tracked task statuses (useful for dashboard)."""
    if REDIS_AVAILABLE:
        return r.hgetall("task_status")
    return dict(_task_status)
