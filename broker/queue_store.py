import copy
import heapq
import logging
import threading
import time
import uuid
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from .models import Job, JobStatus
from .wal import WriteAheadLog

LOGGER = logging.getLogger(__name__)


class QueueError(Exception):
    def __init__(self, message: str, code: str = "invalid_transition") -> None:
        super().__init__(message)
        self.code = code


class QueueStore:
    def __init__(
        self,
        wal_path: str = "data/queue.wal",
        visibility_timeout: float = 15.0,
        max_attempts: int = 5,
        base_delay: float = 1.0,
        boost_interval: float = 60.0,
    ) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, Job] = {}
        self._pending: List[Tuple[float, float, str]] = []
        self._dlq: List[str] = []
        self.visibility_timeout = visibility_timeout
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.boost_interval = boost_interval
        self._wal = WriteAheadLog(wal_path)
        self._replay()

    def enqueue(self, job: Job) -> None:
        with self._lock:
            if job.id in self._jobs:
                raise QueueError("job already exists", "duplicate_job")
            self._append_and_apply({"event": "enqueued", "job": self._serialize(job)})

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            job = self._jobs.get(job_id)
            return copy.deepcopy(job) if job is not None else None

    def dequeue(self, worker_id: str) -> Optional[Job]:
        with self._lock:
            now = time.time()
            deferred: List[Tuple[float, float, str]] = []
            selected: Optional[Job] = None
            while self._pending:
                entry = heapq.heappop(self._pending)
                job = self._jobs.get(entry[2])
                if job is None or job.status is not JobStatus.PENDING:
                    continue
                if job.not_before > now:
                    deferred.append(entry)
                    continue
                selected = job
                break
            for entry in deferred:
                heapq.heappush(self._pending, entry)
            if selected is None:
                return None
            event = {
                "event": "leased",
                "job_id": selected.id,
                "worker_id": worker_id,
                "lease_token": uuid.uuid4().hex,
                "lease_expires_at": now + self.visibility_timeout,
            }
            self._append_and_apply(event)
            return copy.deepcopy(self._jobs[selected.id])

    def ack(self, job_id: str, worker_id: str, lease_token: str) -> None:
        with self._lock:
            job = self._require_lease(job_id, worker_id, lease_token)
            self._append_and_apply({"event": "acked", "job_id": job.id})

    def nack(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        reason: str,
        permanent: bool = False,
    ) -> None:
        with self._lock:
            self._require_lease(job_id, worker_id, lease_token)
            self._fail_locked(job_id, reason, permanent)

    def renew(self, job_id: str, worker_id: str, lease_token: str) -> None:
        with self._lock:
            job = self._require_lease(job_id, worker_id, lease_token)
            self._append_and_apply({
                "event": "lease_renewed",
                "job_id": job_id,
                "lease_expires_at": time.time() + self.visibility_timeout,
            })

    def sweep_expired_leases(self, now: Optional[float] = None) -> int:
        with self._lock:
            current_time = time.time() if now is None else now
            expired = [
                job.id for job in self._jobs.values()
                if job.status is JobStatus.IN_FLIGHT
                and job.lease_expires_at is not None
                and job.lease_expires_at <= current_time
            ]
            for job_id in expired:
                self._fail_locked(job_id, "lease expired")
            return len(expired)

    def list_dlq(self) -> List[Job]:
        with self._lock:
            return [copy.deepcopy(self._jobs[job_id]) for job_id in self._dlq]

    def retry_dlq(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status is not JobStatus.DEAD_LETTERED:
                raise QueueError("job is not in the dead-letter queue", "not_in_dlq")
            self._append_and_apply({"event": "dlq_retried", "job_id": job_id})

    def discard_dlq(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status is not JobStatus.DEAD_LETTERED:
                raise QueueError("job is not in the dead-letter queue", "not_in_dlq")
            self._append_and_apply({"event": "discarded", "job_id": job_id})

    def stats(self) -> Dict[str, int]:
        with self._lock:
            counts = {status.value: 0 for status in JobStatus}
            for job in self._jobs.values():
                counts[job.status.value] += 1
            counts["dlq"] = len(self._dlq)
            return counts

    def _require_lease(self, job_id: str, worker_id: str, lease_token: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise QueueError("unknown job", "unknown_job")
        if job.status is not JobStatus.IN_FLIGHT:
            raise QueueError("job is not in flight", "not_in_flight")
        if job.leased_by != worker_id or job.lease_token != lease_token:
            raise QueueError("lease is not owned by this worker", "lease_conflict")
        return job

    def _fail_locked(self, job_id: str, reason: str, permanent: bool = False) -> None:
        job = self._jobs[job_id]
        attempts = job.attempts + 1
        if permanent or attempts >= self.max_attempts:
            self._append_and_apply({"event": "dead_lettered", "job_id": job_id, "reason": reason})
            return
        delay = self.base_delay * (2 ** attempts)
        self._append_and_apply({
            "event": "retried",
            "job_id": job_id,
            "reason": reason,
            "attempts": attempts,
            "not_before": time.time() + delay,
        })

    def _append_and_apply(self, event: Dict[str, Any]) -> None:
        self._wal.append(event)
        self._apply(event)

    def _apply(self, event: Dict[str, Any]) -> None:
        event_type = event["event"]
        if event_type == "enqueued":
            job = self._deserialize(event["job"])
            self._jobs[job.id] = job
            self._push_pending(job)
        elif event_type == "leased":
            job = self._jobs[event["job_id"]]
            job.status = JobStatus.IN_FLIGHT
            job.leased_by = event["worker_id"]
            job.lease_token = event["lease_token"]
            job.lease_expires_at = event["lease_expires_at"]
        elif event_type == "acked":
            job = self._jobs[event["job_id"]]
            job.status = JobStatus.ACKED
            job.leased_by = None
            job.lease_token = None
            job.lease_expires_at = None
        elif event_type == "lease_renewed":
            self._jobs[event["job_id"]].lease_expires_at = event["lease_expires_at"]
        elif event_type == "retried":
            job = self._jobs[event["job_id"]]
            job.status = JobStatus.PENDING
            job.attempts = event["attempts"]
            job.not_before = event["not_before"]
            job.leased_by = None
            job.lease_token = None
            job.lease_expires_at = None
            self._push_pending(job)
        elif event_type == "dead_lettered":
            job = self._jobs[event["job_id"]]
            job.status = JobStatus.DEAD_LETTERED
            job.attempts += 1
            job.leased_by = None
            job.lease_token = None
            job.lease_expires_at = None
            if job.id not in self._dlq:
                self._dlq.append(job.id)
        elif event_type == "dlq_retried":
            job = self._jobs[event["job_id"]]
            self._dlq.remove(job.id)
            job.status = JobStatus.PENDING
            job.not_before = 0.0
            self._push_pending(job)
        elif event_type == "discarded":
            self._dlq.remove(event["job_id"])
            del self._jobs[event["job_id"]]

    def _push_pending(self, job: Job) -> None:
        key = job.enqueued_at - job.priority * self.boost_interval
        heapq.heappush(self._pending, (key, job.enqueued_at, job.id))

    def _replay(self) -> None:
        for event in self._wal.replay():
            self._apply(event)

    @staticmethod
    def _serialize(job: Job) -> Dict[str, Any]:
        value = asdict(job)
        value["status"] = job.status.value
        return value

    @staticmethod
    def _deserialize(value: Dict[str, Any]) -> Job:
        value = dict(value)
        value["status"] = JobStatus(value["status"])
        return Job(**value)


def sweep_loop(store: QueueStore, interval: float = 1.0) -> None:
    while True:
        time.sleep(interval)
        try:
            store.sweep_expired_leases()
        except Exception:
            LOGGER.exception("lease sweeper failed")
