# broker/queue_store.py
import threading

class QueueStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._pending = []   # plain list for now — swap for heap later
        self._jobs = {}      # job_id -> Job, single source of truth

    def enqueue(self, job):
        with self._lock:
            self._jobs[job.id] = job
            self._pending.append(job.id)

    def dequeue(self):
        with self._lock:
            if not self._pending:
                return None
            job_id = self._pending.pop(0)   # naive FIFO for now, not priority yet
            return self._jobs[job_id]