import argparse
import logging
import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import requests

from .handlers_map import HANDLERS

LOGGER = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        broker_url: str,
        worker_id: str,
        concurrency: int = 4,
        poll_interval: float = 1.0,
        request_timeout: float = 10.0,
        lease_renew_interval: float = 5.0,
    ) -> None:
        self.broker_url = broker_url.rstrip("/")
        self.worker_id = worker_id
        self.poll_interval = poll_interval
        self.request_timeout = request_timeout
        self.lease_renew_interval = lease_renew_interval
        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="job")

    def run(self) -> None:
        LOGGER.info("worker %s started", self.worker_id)
        try:
            while not self._stop.is_set():
                try:
                    response = requests.post(
                        f"{self.broker_url}/dequeue",
                        json={"worker_id": self.worker_id},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    job = response.json().get("job")
                except (requests.RequestException, ValueError):
                    LOGGER.exception("broker dequeue failed")
                    self._stop.wait(self.poll_interval)
                    continue
                if job is None:
                    self._stop.wait(self.poll_interval)
                    continue
                self._executor.submit(self._execute, job)
        finally:
            self._executor.shutdown(wait=True, cancel_futures=False)
            LOGGER.info("worker %s stopped", self.worker_id)

    def stop(self) -> None:
        self._stop.set()

    def _execute(self, job: Dict[str, Any]) -> None:
        job_id = job["id"]
        lease_token = job["lease_token"]
        renew_stop = threading.Event()
        renewer = threading.Thread(
            target=self._renew_loop,
            args=(job_id, lease_token, renew_stop),
            name=f"renew-{job_id[:8]}",
            daemon=True,
        )
        renewer.start()
        try:
            handler = HANDLERS.get(job["job_name"])
            if handler is None:
                self._nack(job_id, lease_token, "unknown job_name", permanent=True)
                return
            handler(job["payload"])
            self._ack(job_id, lease_token)
        except Exception as error:
            LOGGER.exception("job %s failed", job_id)
            self._nack(job_id, lease_token, str(error))
        finally:
            renew_stop.set()

    def _renew_loop(self, job_id: str, lease_token: str, stop: threading.Event) -> None:
        while not stop.wait(self.lease_renew_interval):
            try:
                response = requests.post(
                    f"{self.broker_url}/renew/{job_id}",
                    json={"worker_id": self.worker_id, "lease_token": lease_token},
                    timeout=self.request_timeout,
                )
                if response.status_code == 409:
                    LOGGER.warning("lease for job %s is no longer owned", job_id)
                    return
                response.raise_for_status()
            except requests.RequestException:
                LOGGER.exception("lease renewal failed for job %s", job_id)

    def _ack(self, job_id: str, lease_token: str) -> None:
        response = requests.post(
            f"{self.broker_url}/ack/{job_id}",
            json={"worker_id": self.worker_id, "lease_token": lease_token},
            timeout=self.request_timeout,
        )
        response.raise_for_status()

    def _nack(
        self,
        job_id: str,
        lease_token: str,
        reason: str,
        permanent: bool = False,
    ) -> None:
        response = requests.post(
            f"{self.broker_url}/nack/{job_id}",
            json={
                "worker_id": self.worker_id,
                "lease_token": lease_token,
                "reason": reason,
                "permanent": permanent,
            },
            timeout=self.request_timeout,
        )
        response.raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-url", default=os.getenv("BROKER_URL", "http://localhost:8000"))
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("WORKER_CONCURRENCY", "4")))
    args = parser.parse_args()
    worker = Worker(
        args.broker_url,
        args.worker_id or f"{socket.gethostname()}-{os.getpid()}",
        concurrency=args.concurrency,
    )
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    worker.run()


if __name__ == "__main__":
    main()