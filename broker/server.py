import logging
import threading
from typing import Any, Dict, Optional, Tuple, Type, Union

from flask import Flask, Response, jsonify, request

from broker.queue_store import QueueStore, sweep_loop
from broker.models import Job

app = Flask(__name__)
store = QueueStore()
JsonObject = Dict[str, Any]
RouteResponse = Union[Response, Tuple[Response, int]]


def json_response(code: int = 200,**kwargs) -> Tuple[Response, int]:
    return jsonify(**kwargs), code


def validate_field(
    data: JsonObject,
    field_name: str,
    field_type: Type[Any],
    required: bool = True,
) -> Tuple[Optional[Any], Optional[RouteResponse]]:
    if field_name not in data:
        if required:
            return None, json_response(
                code=400,
                error=f"required field {field_name} is missing"
            )
        return None, None

    value = data[field_name]
    if not isinstance(value, field_type):
        return None, json_response(
            code=400,
            error=f"{field_name} must be a {field_type.__name__}"
        )
    return value, None


@app.post("/enqueue")
def enqueue() -> RouteResponse:
    data = request.get_json(silent=True) or {}
    job_name, error = validate_field(data, "job_name", str)
    if error is not None:
        return error
    payload, error = validate_field(data, "payload", dict, required=False)
    if error is not None:
        return error
    if payload is None:
        payload = {}
    priority, error = validate_field(data, "priority", int, required=False)
    if error is not None:
        return error
    if priority is None:
        priority = 1
    job = Job(job_name=job_name, payload=payload, priority=priority)
    store.enqueue(job)
    return json_response(
        code=201,
        job_id=job.id
    )

@app.post("/dequeue")
def dequeue() -> RouteResponse:
    data = request.get_json(silent=True) or {}
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    job = store.dequeue(worker_id)
    if job is None:
        return json_response(
            code=200,
            job=None
        )
    return json_response(
        code=200,
        job={"id": job.id, "job_name": job.job_name, "payload": job.payload}
    )

@app.post("/ack/<job_id>")
def ack(job_id: str) -> RouteResponse:
    data = request.get_json(silent=True) or {}
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    job = store.get(job_id)

    if job is None:
        return json_response(
            code=404,
            error="unknown job"
        )
    if job.leased_by != worker_id:
        return json_response(
            code=409,
            error="not the current lease holder"
        )

    store.mark_acked(job_id)
    return json_response(
        code=200,
        ok=True
    )


@app.post("/nack/<job_id>")
def nack(job_id: str) -> RouteResponse:
    data = request.get_json(silent=True) or {}
    worker_id, error = validate_field(data, "worker_id", str)
    if error is not None:
        return error
    reason, error = validate_field(data, "reason", str, required=False)
    if error is not None:
        return error
    if reason is None:
        reason = ""
    job = store.get(job_id)

    if job is None:
        return json_response(
            code=404,
            error="unknown job"
        )
    if job.leased_by != worker_id:
        return json_response(
            code=409,
            error="not the current lease holder"
        )

    store.mark_failed(job_id, reason)
    return json_response(
        code=200,
        ok=True
    )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    threading.Thread(target=sweep_loop, args=(store,), daemon=True).start()
    app.run(port=8000, threaded=True)