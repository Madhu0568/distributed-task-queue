# Distributed Task Queue & Job Scheduler

> This project demonstrates backend system design concepts including APIs, data processing, and asynchronous workflows.

## Overview

Built a task queue system supporting asynchronous job execution using a worker-based architecture. Designed REST APIs for task submission, status tracking, and result retrieval. Implemented retry handling for failed tasks and queue monitoring endpoints. Enables concurrent processing using worker threads for improved task throughput.

I started with a simple FIFO queue and kept running into ordering problems with high-priority tasks. That led me to switch to a heap-based priority queue, which solved it cleanly. Then I hit failures during stress testing — tasks were just dying silently — so I added retry logic with a dead-letter queue for tasks that exhaust all retries.

## Features

- **Priority queue** (high / medium / low) using Python's `heapq` — high-priority tasks jump ahead of low-priority ones
- **4 concurrent worker threads** pulling from the shared queue
- **Retry mechanism** — failed tasks are re-enqueued up to 3 times before being moved to the dead-letter queue
- **Dead-letter queue** — stores tasks that failed all retry attempts so they can be inspected
- **REST API** for task submission, status polling, and result retrieval
- **Real-time monitoring dashboard** showing queue depth, worker status, and job completion rates (auto-refreshes every 2 seconds)
- **Redis support** — connects to Redis if available, falls back to in-memory for local development

## Tech Stack

Python · Flask · Redis · REST APIs · Multithreading

## How to Run

```bash
pip install -r requirements.txt
python app.py
```

Open `http://localhost:5000` for the monitoring dashboard.

Optionally start Redis for persistent queue storage:
```bash
redis-server
python worker.py   # run standalone worker process
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/tasks` | Submit a task |
| POST | `/api/tasks/batch` | Submit multiple tasks |
| GET | `/api/tasks/<id>` | Poll task status |
| GET | `/api/tasks/<id>/result` | Get task result |
| GET | `/api/tasks` | List all tasks (filter by `?status=`) |
| GET | `/api/monitor` | Queue depth, worker status, throughput |
| GET | `/api/dead-letter` | Tasks that exhausted all retries |
| DELETE | `/api/queue/clear` | Clear pending queue |

## Example

```bash
# Submit a high-priority task
curl -X POST http://localhost:5000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "compute", "payload": {"number": 500}, "priority": "high"}'

# Response
{"task_id": "a1b2c3d4", "status": "queued", "priority": "high", "queue_position": 1}

# Poll status
curl http://localhost:5000/api/tasks/a1b2c3d4

# Response when done
{"task_id": "a1b2c3d4", "status": "completed", "result": {"sum_of_squares": 41541750}}

# Monitor the queue
curl http://localhost:5000/api/monitor
```

## Supported Task Types

| Type | Payload | Description |
|------|---------|-------------|
| `compute` | `{"number": N}` | Computes sum of squares up to N |
| `transform` | `{"text": "..."}` | Uppercase, reverse, word count |
| `aggregate` | `{"values": [1,2,3]}` | Sum, avg, min, max |
| `simulate_failure` | `{}` | Fails twice then succeeds — tests retry logic |

## Architecture

```
Producer (REST API)
     │
     ▼
Priority Heap Queue  ←── retry re-enqueue
     │
     ▼
Worker Threads (×4)  ──→  Dead-Letter Queue (on max retries)
     │
     ▼
Result stored in task map → polled via GET /api/tasks/<id>
```

## Output

See [sample_output.txt](sample_output.txt) for real API request/response examples including task submission, status polling, dashboard stats, and dead-letter queue inspection.
