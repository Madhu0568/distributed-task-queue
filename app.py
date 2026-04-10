from flask import Flask, request, jsonify
import threading
import time
import uuid
import heapq
from collections import defaultdict
from datetime import datetime

app = Flask(__name__)

task_queue = []
task_lock = threading.Lock()
tasks = {}
dead_letter_queue = []
worker_status = {}
completed_count = 0
failed_count = 0
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
        }


def execute_task(task):
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
            "average": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
        }

    elif task_type == "simulate_failure":
        if task.retries < 2:
            raise Exception("Simulated transient failure")
        return {"message": "Succeeded after retries"}

    else:
        time.sleep(0.02)
        return {"message": f"Processed task type: {task_type}", "payload": payload}


def worker(worker_id):
    global completed_count, failed_count

    worker_status[worker_id] = {"status": "idle", "current_task": None, "tasks_completed": 0}

    while True:
        task = None
        with task_lock:
            if task_queue:
                task = heapq.heappop(task_queue)

        if task is None:
            worker_status[worker_id]["status"] = "idle"
            worker_status[worker_id]["current_task"] = None
            time.sleep(0.1)
            continue

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
                task.status = "queued"
                task.error = str(e)
                with task_lock:
                    heapq.heappush(task_queue, task)
            else:
                task.status = "failed"
                task.error = str(e)
                task.completed_at = datetime.utcnow().isoformat()
                dead_letter_queue.append(task.to_dict())
                failed_count += 1


for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(f"worker-{i+1}",), daemon=True)
    t.start()


@app.route("/api/tasks", methods=["POST"])
def submit_task():
    data = request.get_json()
    if not data or "task_type" not in data:
        return jsonify({"error": "task_type is required"}), 400

    task_id = str(uuid.uuid4())[:8]
    priority = data.get("priority", "medium")
    payload = data.get("payload", {})

    task = Task(task_id, data["task_type"], payload, priority)
    tasks[task_id] = task

    with task_lock:
        heapq.heappush(task_queue, task)

    return jsonify({"task_id": task_id, "status": "queued", "priority": priority}), 201


@app.route("/api/tasks/batch", methods=["POST"])
def submit_batch():
    data = request.get_json()
    if not data or "tasks" not in data:
        return jsonify({"error": "tasks array is required"}), 400

    results = []
    for task_data in data["tasks"]:
        task_id = str(uuid.uuid4())[:8]
        priority = task_data.get("priority", "medium")
        payload = task_data.get("payload", {})
        task_type = task_data.get("task_type", "default")

        task = Task(task_id, task_type, payload, priority)
        tasks[task_id] = task

        with task_lock:
            heapq.heappush(task_queue, task)

        results.append({"task_id": task_id, "status": "queued"})

    return jsonify({"submitted": len(results), "tasks": results}), 201


@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task_status(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task.to_dict())


@app.route("/api/tasks", methods=["GET"])
def list_tasks():
    status_filter = request.args.get("status")
    task_list = [t.to_dict() for t in tasks.values()]
    if status_filter:
        task_list = [t for t in task_list if t["status"] == status_filter]
    return jsonify({"total": len(task_list), "tasks": task_list})


@app.route("/api/tasks/<task_id>/result", methods=["GET"])
def get_task_result(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    if task.status != "completed":
        return jsonify({"error": "Task not yet completed", "status": task.status}), 202
    return jsonify({"task_id": task_id, "result": task.result})


@app.route("/api/monitor", methods=["GET"])
def monitor():
    queue_depth = len(task_queue)
    return jsonify({
        "queue_depth": queue_depth,
        "workers": worker_status,
        "total_tasks": len(tasks),
        "completed": completed_count,
        "failed": failed_count,
        "dead_letter_queue_size": len(dead_letter_queue),
    })


@app.route("/api/dead-letter", methods=["GET"])
def get_dead_letter():
    return jsonify({"count": len(dead_letter_queue), "tasks": dead_letter_queue})


@app.route("/")
def dashboard():
    return app.send_static_file("index.html")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
