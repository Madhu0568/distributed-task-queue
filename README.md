# Distributed Task Queue & Job Scheduler

A distributed task queue system built with Python Flask supporting concurrent job execution across worker threads with priority-based scheduling, retry mechanisms, and real-time monitoring.

## Features

- **Priority-based job scheduling** (high, medium, low) with heap-based queue
- **Concurrent execution** across multiple worker threads (500+ tasks/minute throughput)
- **Retry mechanisms** with configurable max retries and dead-letter queue handling
- **RESTful API** for task submission, status polling, and result retrieval
- **Real-time monitoring dashboard** with 2-second auto-refresh
- **Batch task submission** for bulk processing
- **Multiple task types**: compute, transform, aggregate, and custom

## Tech Stack

- Python 3.x
- Flask (REST API framework)
- Threading (concurrent worker execution)
- Heap-based priority queue

## Setup & Run

```bash
pip install -r requirements.txt
python app.py
```

The server starts at `http://localhost:5000`

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/tasks` | Submit a single task |
| POST | `/api/tasks/batch` | Submit multiple tasks |
| GET | `/api/tasks` | List all tasks (optional `?status=` filter) |
| GET | `/api/tasks/<id>` | Get task status |
| GET | `/api/tasks/<id>/result` | Get task result |
| GET | `/api/monitor` | Real-time queue and worker metrics |
| GET | `/api/dead-letter` | View failed tasks in dead-letter queue |

## Example Usage

### Submit a task
```bash
curl -X POST http://localhost:5000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "compute", "payload": {"number": 1000}, "priority": "high"}'
```

### Submit batch tasks
```bash
curl -X POST http://localhost:5000/api/tasks/batch \
  -H "Content-Type: application/json" \
  -d '{"tasks": [{"task_type": "compute", "payload": {"number": 100}}, {"task_type": "transform", "payload": {"text": "hello"}}]}'
```

## Architecture

- **Producer**: REST API endpoints accept task submissions
- **Queue**: Thread-safe priority heap queue
- **Workers**: 4 concurrent worker threads processing tasks
- **Scheduler**: Priority-based with retry logic (max 3 retries)
- **Dead Letter Queue**: Failed tasks stored for inspection
- **Monitor**: Real-time dashboard with queue depth, worker status, and completion rates
