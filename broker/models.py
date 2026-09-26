from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
import time
import uuid

# Job status can be 4 type (PENDING / IN_FLIGHT / ACKED / DEAD_LETTERED)
class JobStatus(Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    ACKED = "acked"
    DEAD_LETTERED = "dead_lettered"

@dataclass
class Job:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_name: str = ""                          # e.g. "resize_image" — looked up in handler map
    payload: Dict[str, Any] = field(default_factory=dict)   # arguments passed to the handler
    priority: int = 1                        # base priority, before age-boosting
    status: JobStatus = JobStatus.PENDING     
    enqueued_at: float = field(default_factory=time.time)  # used for age-based priority
    attempts: int = 0                          # incremented on each nack/lease-timeout
    not_before: float = 0.0                    # retry backoff — can't be leased before this time
    leased_by: Optional[str] = None           # which worker currently holds the lease
    lease_expires_at: Optional[float] = None
    lease_token: Optional[str] = None