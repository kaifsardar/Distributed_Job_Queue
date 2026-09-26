import logging
import os
import threading
from typing import Any, Dict, Optional, Tuple, Type, Union

from flask import Flask, Response, jsonify, request

from .models import Job
from .queue_store import QueueError, QueueStore, sweep_loop

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger(__name__)
app = Flask(__name__)
store = QueueStore(
    wal_path=os.getenv("WAL_PATH", "data/queue.wal"),
    visibility_timeout=float(os.getenv("VISIBILITY_TIMEOUT", "15")),
    max_attempts=int(os.getenv("MAX_ATTEMPTS", "5")),
    base_delay=float(os.getenv("RETRY_BASE_DELAY", "1")),
)
JsonObject = Dict[str, Any]
RouteResponse = Union[Response, Tuple[Response, int]]


def json_response(code: int = 200, **kwargs: Any) -> Tuple[Response, int]:
    return jsonify(**kwargs), code


def validate_field(
    data: JsonObject,
    field_name: str,
    field_type: Type[Any],
    required: bool = True,
) -> Tuple[Optional[Any], Optional[RouteResponse]]:
    if field_name not in data:
        if required:
            return None, json_response(400, error=f"required field {field_name} is missing")
        return None, None
    value = data[field_name]
    if not isinstance(value, field_type) or (field_type is int and isinstance(value, bool)):
        return None, json_response(400, error=f"{field_name} must be a {field_type.__name__}")
    return value, None


def request_json() -> Tuple[Optional[JsonObject], Optional[RouteResponse]]:
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return None, json_response(400, error="request body must be a JSON object")
    return data, None


def serialize_job(job: Job) -> JsonObject:
    return {
        "id": job.id,
        "job_name": job.job_name,
        "payload": job.payload,
        "priority": job.priority,
        "status": job.status.value,
        "attempts": job.attempts,
        "not_before": job.not_before,
        "leased_by": job.leased_by,
        "lease_expires_at": job.lease_expires_at,
        "lease_token": job.lease_token,
    }


@app.errorhandler(404)
def not_found(_: Any) -> RouteResponse:
    return json_response(404, error="route not found")


@app.errorhandler(405)
def method_not_allowed(_: Any) -> RouteResponse:
    return json_response(405, error="method not allowed")


@app.get("/healthz")
def healthz() -> RouteResponse:
    return json_response(200, status="ok")


@app.get("/readyz")
def readyz() -> RouteResponse:
    return json_response(200, status="ready")


@app.post("/enqueue")
def enqueue() -> RouteResponse:
    data, error = request_json()
    if error is not None:
        return error
    assert data is not None
    job_name, error = validate_field(data, "job_name", str)
    if error is not None:
        return error
    payload, error = validate_field(data, "payload", dict, required=False)
    if error is not None:
        return error
    priority, error = validate_field(data, "priority", int, required=False)
    if error is not None:
        return error
    job = Job(
        job_name=job_name,
        payload=payload if payload is not None else {},
        priority=priority if priority is not None else 1,
    )
    try:
        store.enqueue(job)
    except QueueError as exc:
        return json_response(409, error=str(exc), error_code=exc.code)
    return json_response(201, job_id=job.id)


@app.post("/dequeue")
def dequeue() -> RouteResponse:
    data, error = request_json()
    if error is not None:
        return error
    assert data is not None
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    job = store.dequeue(worker_id)
    if job is None:
        return json_response(200, job=None)
    return json_response(200, job=serialize_job(job))


@app.post("/ack/<job_id>")
def ack(job_id: str) -> RouteResponse:
    data, error = request_json()
    if error is not None:
        return error
    assert data is not None
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    lease_token, error = validate_field(data, "lease_token", str)
    if error is not None:
        return error
    try:
        store.ack(job_id, worker_id, lease_token)
    except QueueError as exc:
        status = 404 if exc.code == "unknown_job" else 409
        return json_response(status, error=str(exc), error_code=exc.code)
    return json_response(200, ok=True)


@app.post("/nack/<job_id>")
def nack(job_id: str) -> RouteResponse:
    data, error = request_json()
    if error is not None:
        return error
    assert data is not None
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    lease_token, error = validate_field(data, "lease_token", str)
    if error is not None:
        return error
    reason, error = validate_field(data, "reason", str, required=False)
    if error is not None:
        return error
    permanent, error = validate_field(data, "permanent", bool, required=False)
    if error is not None:
        return error
    try:
        store.nack(
            job_id,
            worker_id,
            lease_token,
            reason or "worker reported failure",
            permanent=bool(permanent),
        )
    except QueueError as exc:
        status = 404 if exc.code == "unknown_job" else 409
        return json_response(status, error=str(exc), error_code=exc.code)
    return json_response(200, ok=True)


@app.post("/renew/<job_id>")
def renew(job_id: str) -> RouteResponse:
    data, error = request_json()
    if error is not None:
        return error
    assert data is not None
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    lease_token, error = validate_field(data, "lease_token", str)
    if error is not None:
        return error
    try:
        store.renew(job_id, worker_id, lease_token)
    except QueueError as exc:
        status = 404 if exc.code == "unknown_job" else 409
        return json_response(status, error=str(exc), error_code=exc.code)
    return json_response(200, ok=True)


@app.get("/jobs/<job_id>")
def get_job(job_id: str) -> RouteResponse:
    job = store.get(job_id)
    if job is None:
        return json_response(404, error="unknown job")
    return json_response(200, job=serialize_job(job))


@app.get("/queue/stats")
def queue_stats() -> RouteResponse:
    return json_response(200, stats=store.stats())


@app.get("/dlq")
def dlq() -> RouteResponse:
    return json_response(200, jobs=[serialize_job(job) for job in store.list_dlq()])


@app.post("/dlq/<job_id>/retry")
def retry_dlq(job_id: str) -> RouteResponse:
    try:
        store.retry_dlq(job_id)
    except QueueError as exc:
        return json_response(404, error=str(exc), error_code=exc.code)
    return json_response(200, ok=True)


@app.delete("/dlq/<job_id>")
def discard_dlq(job_id: str) -> RouteResponse:
    try:
        store.discard_dlq(job_id)
    except QueueError as exc:
        return json_response(404, error=str(exc), error_code=exc.code)
    return json_response(200, ok=True)


_sweeper = threading.Thread(target=sweep_loop, args=(store,), name="lease-sweeper", daemon=True)
_sweeper.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), threaded=True)
