"""
Distributed Task Queue & Job Scheduler
Supports Redis (production) with in-memory fallback (development/testing)
Uses queue_manager.py for Redis queue operations and worker.py for task execution.
"""
from flask import Flask, request, jsonify
import threading
import time
import uuid
import heapq
from datetime import datetime
from collections import deque
from queue_manager import (
    queue_length as redis_queue_length,
    all_statuses,
    REDIS_AVAILABLE,
    add_task as redis_add_task,
    get_status as redis_get_status,
    save_status as redis_save_status,
)

# In-memory state (used when Redis is not available)
task_queue = []          # min-heap (priority queue)
task_lock = threading.Lock()
tasks = {}               # task_id -> Task
dead_letter_queue = []
worker_status = {}
completed_count = 0
failed_count = 0
start_time = datetime.utcnow()

NUM_WORKERS = 4
MAX_RETRIES = 3
PRIORITY_MAP = {"high": 1, "medium": 2, "low": 3}


class Task:
    def __init__(self, task_id, task_type, payload, priority="medium"):
        self.task_id = task_id
        self.task_type = task_type
        self.payload = payload
        self.priority = PRIORITY_MAP.get(priority, 2)
        self.priority_label = priority
        self.status = "queued"
        self.result = None
        self.created_at = datetime.utcnow().isoformat()
        self.started_at = None
        self.completed_at = None
        self.retries = 0
        self.worker_id = None
        self.error = None
        self.scheduling_latency_ms = None

    def __lt__(self, other):
        return self.priority < other.priority

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "payload": self.payload,
            "priority": self.priority_label,
            "status": self.status,
            "result": self.result,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "retries": self.retries,
            "worker_id": self.worker_id,
            "error": self.error,
            "scheduling_latency_ms": self.scheduling_latency_ms,
        }


def execute_task(task):
    """Execute a task based on its type and return a result."""
    task_type = task.task_type
    payload = task.payload

    if task_type == "compute":
        n = payload.get("number", 10)
        result = sum(i * i for i in range(n))
        time.sleep(0.05)
        return {"sum_of_squares": result, "input": n}

    elif task_type == "transform":
        text = payload.get("text", "")
        time.sleep(0.03)
        return {
            "original": text,
            "upper": text.upper(),
            "reversed": text[::-1],
            "word_count": len(text.split()),
        }

    elif task_type == "aggregate":
        values = payload.get("values", [])
        time.sleep(0.04)
        if not values:
            return {"error": "No values provided"}
        return {
            "count": len(values),
            "sum": sum(values),
            "average": round(sum(values) / len(values), 2),
            "min": min(values),
            "max": max(values),
        }

    elif task_type == "simulate_failure":
        # Used to test retry logic — succeeds on the 3rd attempt
        if task.retries < 2:
            raise Exception("Simulated transient failure — will retry")
        return {"message": "Succeeded after retries", "attempts": task.retries + 1}

    else:
        time.sleep(0.02)
        return {"message": f"Task type '{task_type}' processed", "payload": payload}


def worker(worker_id):
    """Worker thread: polls priority queue and executes tasks with retry logic."""
    global completed_count, failed_count

    worker_status[worker_id] = {
        "status": "idle",
        "current_task": None,
        "tasks_completed": 0,
        "tasks_failed": 0,
    }

    while True:
        task = None
        with task_lock:
            if task_queue:
                task = heapq.heappop(task_queue)

        if task is None:
            worker_status[worker_id]["status"] = "idle"
            worker_status[worker_id]["current_task"] = None
            time.sleep(0.05)
            continue

        # Record scheduling latency (time from submission to start)
        enqueue_time = datetime.fromisoformat(task.created_at)
        latency_ms = round((datetime.utcnow() - enqueue_time).total_seconds() * 1000, 1)
        task.scheduling_latency_ms = latency_ms

        worker_status[worker_id]["status"] = "busy"
        worker_status[worker_id]["current_task"] = task.task_id
        task.status = "running"
        task.started_at = datetime.utcnow().isoformat()
        task.worker_id = worker_id

        try:
            result = execute_task(task)
            task.status = "completed"
            task.result = result
            task.completed_at = datetime.utcnow().isoformat()
            completed_count += 1
            worker_status[worker_id]["tasks_completed"] += 1

        except Exception as e:
            task.retries += 1
            if task.retries < MAX_RETRIES:
                # Re-enqueue with same priority for retry
                task.status = "queued"
                task.error = f"Retry {task.retries}/{MAX_RETRIES}: {str(e)}"
                with task_lock:
                    heapq.heappush(task_queue, task)
            else:
                # Move to dead-letter queue after max retries exhausted
                task.status = "failed"
                task.error = str(e)
                task.completed_at = datetime.utcnow().isoformat()
                dead_letter_queue.append(task.to_dict())
                failed_count += 1
                worker_status[worker_id]["tasks_failed"] += 1


# Start worker threads
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(f"worker-{i+1}",), daemon=True)
    t.start()


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/api/tasks", methods=["POST"])
def submit_task():
    """Submit a single task to the queue."""
    data = request.get_json()
    if not data or "task_type" not in data:
        return jsonify({"error": "task_type is required"}), 400

    task_id = str(uuid.uuid4())[:8]
    priority = data.get("priority", "medium")
    if priority not in PRIORITY_MAP:
        return jsonify({"error": "priority must be high, medium, or low"}), 400

    task = Task(task_id, data["task_type"], data.get("payload", {}), priority)
    tasks[task_id] = task

    with task_lock:
        heapq.heappush(task_queue, task)

    return jsonify({
        "task_id": task_id,
        "status": "queued",
        "priority": priority,
        "queue_position": len(task_queue),
    }), 201


@app.route("/api/tasks/batch", methods=["POST"])
def submit_batch():
    """Submit multiple tasks in one request."""
    data = request.get_json()
    if not data or "tasks" not in data:
        return jsonify({"error": "tasks array is required"}), 400

    results = []
    for item in data["tasks"]:
        task_id = str(uuid.uuid4())[:8]
        priority = item.get("priority", "medium")
        task = Task(task_id, item.get("task_type", "default"), item.get("payload", {}), priority)
        tasks[task_id] = task
        with task_lock:
            heapq.heappush(task_queue, task)
        results.append({"task_id": task_id, "status": "queued"})

    return jsonify({"submitted": len(results), "tasks": results}), 201


@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task_status(task_id):
    """Poll status and result of a specific task."""
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task.to_dict())


@app.route("/api/tasks", methods=["GET"])
def list_tasks():
    """List all tasks with optional status filter."""
    status_filter = request.args.get("status")
    task_list = [t.to_dict() for t in tasks.values()]
    if status_filter:
        task_list = [t for t in task_list if t["status"] == status_filter]
    task_list.sort(key=lambda x: x["created_at"], reverse=True)
    return jsonify({"total": len(task_list), "tasks": task_list})


@app.route("/api/tasks/<task_id>/result", methods=["GET"])
def get_task_result(task_id):
    """Retrieve the result of a completed task."""
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    if task.status != "completed":
        return jsonify({"error": "Task not yet completed", "status": task.status}), 202
    return jsonify({"task_id": task_id, "result": task.result})


@app.route("/api/monitor", methods=["GET"])
def monitor():
    """Real-time queue depth, worker status, and job completion rates."""
    uptime_seconds = (datetime.utcnow() - start_time).total_seconds()
    throughput = round(completed_count / (uptime_seconds / 60), 1) if uptime_seconds > 0 else 0

    # Average scheduling latency across completed tasks
    latencies = [
        t.scheduling_latency_ms for t in tasks.values()
        if t.scheduling_latency_ms is not None
    ]
    avg_latency = round(sum(latencies) / len(latencies), 1) if latencies else 0

    return jsonify({
        "queue_depth": len(task_queue),
        "workers": worker_status,
        "total_tasks": len(tasks),
        "completed": completed_count,
        "failed": failed_count,
        "dead_letter_queue_size": len(dead_letter_queue),
        "throughput_per_minute": throughput,
        "avg_scheduling_latency_ms": avg_latency,
        "redis_connected": REDIS_AVAILABLE,
        "uptime_seconds": round(uptime_seconds),
    })


@app.route("/api/dead-letter", methods=["GET"])
def get_dead_letter():
    """View tasks that exhausted all retries."""
    return jsonify({"count": len(dead_letter_queue), "tasks": dead_letter_queue})


@app.route("/api/queue/clear", methods=["DELETE"])
def clear_queue():
    """Clear all pending tasks from the queue (admin use)."""
    with task_lock:
        cleared = len(task_queue)
        task_queue.clear()
    return jsonify({"cleared": cleared})


@app.route("/api/dashboard")
def dashboard_stats():
    """
    Lightweight dashboard endpoint — shows queue state.
    Uses Redis queue length when Redis is available, falls back to in-memory.
    """
    in_memory_depth = len(task_queue)
    redis_depth = redis_queue_length() if REDIS_AVAILABLE else 0
    effective_depth = redis_depth if REDIS_AVAILABLE else in_memory_depth

    status_counts = {"queued": 0, "running": 0, "completed": 0, "failed": 0}
    for task in tasks.values():
        s = task.status
        if s in status_counts:
            status_counts[s] += 1

    return jsonify({
        "queue_length": effective_depth,
        "redis_connected": REDIS_AVAILABLE,
        "task_counts": status_counts,
        "dead_letter_count": len(dead_letter_queue),
        "active_workers": sum(1 for w in worker_status.values() if w["status"] == "busy"),
        "total_workers": NUM_WORKERS,
    })


@app.route("/")
def index():
    return app.send_static_file("index.html")


if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
