from dataclasses import dataclass
import logging
import threading
import time
from typing import Dict, List, Optional

from broker.models import Job, JobStatus

MAX_ATTEMPTS: int = 5
BASE_DELAY: float = 1.0
logger = logging.getLogger(__name__)


@dataclass
class Event:
    event_type: str
    job_id: str
    reason: str = ""
    attempts: int = 0

class QueueStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: List[str] = []   # plain list for now — swap for heap later
        self._jobs: Dict[str, Job] = {}  # job_id -> Job, single source of truth
        self._wal: List[Event] = []

    def enqueue(self, job: Job) -> None:
        with self._lock:
            self._jobs[job.id] = job
            self._pending.append(job.id)
            logger.info("--Enqueued job %s: %s", job.id, job)

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def dequeue(self, worker_id: str) -> Optional[Job]:
        with self._lock:
            if not self._pending:
                return None
            job_id = self._pending.pop(0)   # naive FIFO for now, not priority yet
            job = self._jobs[job_id]
            job.status = JobStatus.IN_FLIGHT
            job.leased_by = worker_id
            job.lease_expires_at = time.time() + 15
            logger.info("--Dequeued job %s for worker %s: %s", job.id, worker_id, job)
            return job

    def sweep_expired_leases(self, now: Optional[float] = None) -> int:
        """Return expired in-flight jobs to the pending queue."""
        with self._lock:
            current_time = time.time() if now is None else now
            reclaimed = 0
            for job_id, job in self._jobs.items():
                if (
                    job.status is JobStatus.IN_FLIGHT
                    and job.lease_expires_at is not None
                    and job.lease_expires_at < current_time
                ):
                    logger.info("--Lease expired for job %s. Reclaiming it.", job.id)
                    self._mark_failed_locked(job_id, "lease expired")
                    reclaimed += 1
            return reclaimed

    def mark_acked(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = JobStatus.ACKED
            job.leased_by = None
            job.lease_expires_at = None
            self._wal.append(Event("acked", job_id))

    def mark_failed(self, job_id: str, reason: str) -> None:
        with self._lock:
            self._mark_failed_locked(job_id, reason)

    def _mark_failed_locked(self, job_id: str, reason: str) -> None:
        job = self._jobs[job_id]
        job.attempts += 1
        job.leased_by = None
        job.lease_expires_at = None
        if job.attempts >= MAX_ATTEMPTS:
            job.status = JobStatus.DEAD_LETTERED
            logger.info(
                "--Job %s has reached max attempts (%s) and is dead-lettered.",
                job.id,
                MAX_ATTEMPTS,
            )
            self._wal.append(Event("dead_lettered", job_id, reason))
        else:
            job.status = JobStatus.PENDING
            delay = BASE_DELAY * (2 ** job.attempts)
            job.not_before = time.time() + delay
            self._pending.append(job_id)
            logger.info(
                "--Job %s failed (attempt %s). It will be retried after %.2f seconds.",
                job.id,
                job.attempts,
                delay,
            )
            self._wal.append(Event("retried", job_id, reason, job.attempts))


def sweep_loop(store: QueueStore, interval: float = 10.0) -> None:
    while True:
        time.sleep(interval)
        store.sweep_expired_leases()