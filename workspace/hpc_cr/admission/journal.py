"""接纳器独立的只追加事件日志。

与作业子系统的 ``journal.jsonl`` **物理隔离** (``admission-journal.jsonl``),
因此接纳器的事件重放、seq 水位与持久化行为完全自成一体, 不改变既有作业
抢占/检查点恢复的任何事件流与恢复逻辑。语义与 persistence.Journal 一致:

  * 单调递增 seq, 写入 flush+fsync, 崩溃最多丢最后半行;
  * ``data_dir=None`` 退化为纯内存实现。
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Iterator

ADMISSION_JOURNAL_NAME = "admission-journal.jsonl"


class AdmissionJournal:
    def __init__(self, data_dir: str | None = None):
        self._data_dir = data_dir
        self._lock = threading.Lock()
        self._seq = 0
        self._events: list[dict] = []
        self._fp = None
        if data_dir is not None:
            os.makedirs(data_dir, exist_ok=True)
            self._path = os.path.join(data_dir, ADMISSION_JOURNAL_NAME)
            if os.path.exists(self._path):
                with open(self._path, "r", encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            evt = json.loads(line)
                        except json.JSONDecodeError:
                            break  # 崩溃残留半行: 之前的全部有效
                        self._events.append(evt)
                        if isinstance(evt.get("seq"), int):
                            self._seq = max(self._seq, evt["seq"])
            self._fp = open(self._path, "a", encoding="utf-8")

    def append(self, event_type: str, detail: dict[str, Any]) -> dict:
        with self._lock:
            self._seq += 1
            evt = {"seq": self._seq,
                   "ts": detail.get("ts") or _now(),
                   "type": event_type,
                   **{k: v for k, v in detail.items() if k != "ts"}}
            line = json.dumps(evt, ensure_ascii=False, sort_keys=True)
            if self._fp is not None:
                self._fp.write(line + "\n")
                self._fp.flush()
                os.fsync(self._fp.fileno())
            self._events.append(evt)
            return evt

    def replay(self) -> Iterator[dict]:
        with self._lock:
            events = sorted(self._events, key=lambda e: e.get("seq", 0))
        yield from events

    @property
    def last_seq(self) -> int:
        with self._lock:
            return self._seq

    @property
    def persistent(self) -> bool:
        return self._data_dir is not None

    def close(self) -> None:
        with self._lock:
            if self._fp is not None:
                self._fp.flush()
                self._fp.close()
                self._fp = None


def _now() -> float:
    import time
    return time.time()
