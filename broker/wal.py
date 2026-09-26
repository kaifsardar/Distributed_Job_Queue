import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, Iterator


class WriteAheadLog:
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.touch(exist_ok=True)

    def append(self, event: Dict[str, Any]) -> None:
        line = json.dumps(event, separators=(",", ":"), sort_keys=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())

    def replay(self) -> Iterator[Dict[str, Any]]:
        with self._path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
            for line_number, line in enumerate(lines, start=1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    if line_number == len(lines):
                        break
                    raise ValueError(f"invalid WAL event at line {line_number}")
                if not isinstance(event, dict):
                    raise ValueError(f"WAL event at line {line_number} is not an object")
                yield event