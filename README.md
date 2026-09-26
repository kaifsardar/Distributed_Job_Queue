# Distributed Job Queue

A single-broker, at-least-once job queue implemented in Python. Queue state is protected by a process-local lock and persisted with an fsync-backed JSON-lines WAL.

## Run the broker

From the repository root:

```bash
./.venv/bin/pip install -r requirements.txt
WAL_PATH=data/queue.wal ./.venv/bin/python -m broker.server
```

Use one broker process. The queue state is local to that process.

For a production WSGI server, install the requirements and run one worker process with multiple threads:

```bash
gunicorn --workers 1 --threads 8 broker.server:app
```

## Run a worker

In another terminal:

```bash
./.venv/bin/python -m workers.worker --broker-url http://localhost:8000 --concurrency 4
```

## Enqueue a job

The request must declare JSON content:

```bash
curl --json '{"job_name":"resize_image","payload":{"path":"sample.png"},"priority":1}' \
  http://localhost:8000/enqueue
```

The worker leases jobs with a token, renews long-running leases, and acknowledges or negatively acknowledges the job after execution.

## Operational endpoints

```text
GET  /healthz
GET  /readyz
GET  /queue/stats
GET  /jobs/<job_id>
GET  /dlq
POST /dlq/<job_id>/retry
DELETE /dlq/<job_id>
```

Important guarantees are at-least-once delivery, atomic lease ownership, exponential retry backoff, dead-lettering, priority scheduling with age boosting, and WAL replay after restart. Exactly-once delivery, authentication, broker clustering, and WAL compaction are intentionally outside this version.
