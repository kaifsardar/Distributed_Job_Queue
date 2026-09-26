# worker/run_worker.py
import requests, time, socket, os, argparse
from handlers_map import HANDLERS

parser = argparse.ArgumentParser()
parser.add_argument("--broker-url", default="http://localhost:8000")
parser.add_argument("--worker-id", default=None)
args = parser.parse_args()

BROKER_URL = args.broker_url
WORKER_ID = args.worker_id or f"{socket.gethostname()}-{os.getpid()}"

def poll_loop():
    print(f"worker {WORKER_ID} starting, polling {BROKER_URL}")
    while True:
        resp = requests.post(f"{BROKER_URL}/dequeue", json={"worker_id": WORKER_ID})
        job = resp.json().get("job")

        if job is None:
            time.sleep(5)
            continue

        print(f"[{WORKER_ID}] leased job {job['id']} ({job['job_name']})")
        handler = HANDLERS.get(job["job_name"])

        if handler is None:
            requests.post(f"{BROKER_URL}/nack/{job['id']}",
                          json={"worker_id": WORKER_ID, "reason": "unknown job_name"})
            continue

        try:
            handler(job["payload"])
            requests.post(f"{BROKER_URL}/ack/{job['id']}", json={"worker_id": WORKER_ID})
            print(f"[{WORKER_ID}] acked {job['id']}")
        except Exception as e:
            requests.post(f"{BROKER_URL}/nack/{job['id']}",
                          json={"worker_id": WORKER_ID, "reason": str(e)})
            print(f"[{WORKER_ID}] nacked {job['id']}: {e}")

if __name__ == "__main__":
    poll_loop()