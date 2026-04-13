"""
Worker — pulls tasks from the queue and processes them with retry logic.
Run this as a standalone process: python worker.py
"""
import time
import threading
from queue_manager import add_task, get_task, save_status

MAX_RETRIES = 3


def process_task(task):
    """
    Execute a single task. Raises an exception to simulate failure for testing.
    Replace this logic with your real task processing code.
    """
    task_type = task.get("task_type", "default")
    payload = task.get("payload", {})

    if task_type == "compute":
        n = payload.get("number", 10)
        result = sum(i * i for i in range(n))
        return {"sum_of_squares": result, "input": n}

    elif task_type == "transform":
        text = payload.get("text", "")
        return {
            "original": text,
            "upper": text.upper(),
            "reversed": text[::-1],
            "word_count": len(text.split()),
        }

    elif task_type == "aggregate":
        values = payload.get("values", [])
        if not values:
            raise ValueError("No values provided")
        return {
            "count": len(values),
            "sum": sum(values),
            "average": round(sum(values) / len(values), 2),
            "min": min(values),
            "max": max(values),
        }

    elif task_type == "simulate_failure":
        raise Exception("Simulated task failure — triggers retry logic")

    else:
        return {"message": f"Processed task type: {task_type}", "payload": payload}


def run_worker(worker_id="worker-1"):
    """
    Main worker loop: continuously polls the queue, processes tasks,
    and re-enqueues failed tasks up to MAX_RETRIES times.
    """
    print(f"[{worker_id}] started")

    while True:
        task = get_task()

        if task is None:
            time.sleep(0.1)
            continue

        task_id = task.get("task_id", "unknown")
        print(f"[{worker_id}] processing task {task_id} (type={task.get('task_type')})")
        save_status(task_id, "running")

        try:
            result = process_task(task)
            save_status(task_id, "completed")
            print(f"[{worker_id}] completed {task_id}: {result}")

        except Exception as e:
            retry_count = task.get("retry", 0)

            if retry_count < MAX_RETRIES:
                task["retry"] = retry_count + 1
                save_status(task_id, f"retrying ({task['retry']}/{MAX_RETRIES})")
                print(f"[{worker_id}] task {task_id} failed — retry {task['retry']}/{MAX_RETRIES}")
                add_task(task, priority=task.get("priority", 1))
            else:
                save_status(task_id, "failed")
                print(f"[{worker_id}] task {task_id} permanently failed: {e}")


def start_workers(num_workers=4):
    """Start multiple worker threads."""
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=run_worker, args=(f"worker-{i+1}",), daemon=True)
        t.start()
        threads.append(t)
    print(f"Started {num_workers} worker threads")
    return threads


if __name__ == "__main__":
    workers = start_workers(num_workers=4)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Workers stopped")
