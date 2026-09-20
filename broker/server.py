# broker/server.py
from flask import Flask, request, jsonify

from broker.queue_store import QueueStore
from broker.models import Job

app = Flask(__name__)
store = QueueStore()

@app.post("/enqueue")
def enqueue():
    data = request.json
    job = Job(job_name=data["job_name"], payload=data.get("payload", {}), priority=data.get("priority", 1))
    store.enqueue(job)
    return jsonify({"job_id": job.id}), 201

@app.post("/dequeue")
def dequeue():
    job = store.dequeue()
    if job is None:
        return jsonify({"job": None}), 200
    return jsonify({"job": {"id": job.id, "job_name": job.job_name, "payload": job.payload}}), 200

if __name__ == "__main__":
    app.run(port=8000, threaded=True)   # threaded=True is critical — this is your Section 7 decision