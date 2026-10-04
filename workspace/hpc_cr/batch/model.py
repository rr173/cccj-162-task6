"""批接纳器领域模型: 资源向量、租户、异构机器、成组任务。"""

from __future__ import annotations

import dataclasses
import time
from dataclasses import dataclass, field

# 资源维度 (每个分配单元, 即每台机器, 需要的量)
RESOURCE_KEYS = ("cpu", "mem", "gpu")

# 任务状态
QUEUED = "QUEUED"            # 等候接纳
RUNNING = "RUNNING"          # 已拿到全部容量并启动
COMPLETED = "COMPLETED"      # 正常完成
FAILED = "FAILED"            # 失败终态
CANCELLED = "CANCELLED"      # 用户撤回 (仅 QUEUED 可撤回)
TERMINATED = "TERMINATED"    # 用户/系统终止运行项并归还容量

ACTIVE_STATES = (QUEUED, RUNNING)
TERMINAL_STATES = (COMPLETED, FAILED, CANCELLED, TERMINATED)

# 机器状态
ENABLED = "ENABLED"
DRAINING = "DRAINING"        # 不再接纳新任务, 运行项继续直至结束
REMOVED = "REMOVED"

TIMELINE_LIMIT = 200


def _now() -> float:
    return time.time()


@dataclass(frozen=True)
class Resource:
    """单个分配单元 (一台机器) 上需要/具备的资源量。"""

    cpu: int = 0
    mem: int = 0
    gpu: int = 0

    @classmethod
    def from_dict(cls, d: dict | None) -> "Resource":
        d = d or {}
        return cls(cpu=int(d.get("cpu", 0)), mem=int(d.get("mem", 0)),
                   gpu=int(d.get("gpu", 0)))

    def to_dict(self) -> dict:
        return {"cpu": self.cpu, "mem": self.mem, "gpu": self.gpu}

    def fits_in(self, other: "Resource") -> bool:
        return (self.cpu <= other.cpu and self.mem <= other.mem
                and self.gpu <= other.gpu)

    def is_positive(self) -> bool:
        return self.cpu > 0 or self.mem > 0 or self.gpu > 0


@dataclass
class Tenant:
    tenant_id: str
    weight: float
    created_ts: float
    # 加权公平虚拟时间基线; 每接纳一个任务 key += 1/weight。
    # *首次入队* 时对齐到在场活跃租户的最大键值, 避免插队/被压队尾。
    fair_key: float = 0.0
    admitted: int = 0
    # 首次入队对齐是否已完成
    baseline_aligned: bool = False

    def to_dict(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "weight": self.weight,
            "created_ts": self.created_ts,
            "fair_key": self.fair_key,
            "admitted": self.admitted,
            "baseline_aligned": self.baseline_aligned,
        }


@dataclass
class Allocation:
    """一台机器对一个任务的容量占用账目。"""

    machine_id: str
    cpu: int
    mem: int
    gpu: int
    allocated_ts: float

    def to_dict(self) -> dict:
        return {
            "machine_id": self.machine_id,
            "cpu": self.cpu,
            "mem": self.mem,
            "gpu": self.gpu,
            "allocated_ts": self.allocated_ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Allocation":
        return cls(
            machine_id=d["machine_id"], cpu=int(d["cpu"]), mem=int(d["mem"]),
            gpu=int(d["gpu"]), allocated_ts=float(d["allocated_ts"]))


@dataclass
class Machine:
    machine_id: str
    total: Resource
    labels: dict[str, str] = field(default_factory=dict)
    state: str = ENABLED
    created_ts: float = field(default_factory=_now)
    # machine_id -> Allocation (当前占用); 每台机器对同一任务至多一条
    allocations: dict[str, Allocation] = field(default_factory=dict)
    # 容量变化审计: {"ts", "kind", "total", "detail"}
    timeline: list[dict] = field(default_factory=list)

    # ----------------------------------------------------------- capacity
    def used(self) -> Resource:
        cpu = sum(a.cpu for a in self.allocations.values())
        mem = sum(a.mem for a in self.allocations.values())
        gpu = sum(a.gpu for a in self.allocations.values())
        return Resource(cpu=cpu, mem=mem, gpu=gpu)

    def free(self) -> Resource:
        u = self.used()
        return Resource(cpu=self.total.cpu - u.cpu,
                        mem=self.total.mem - u.mem,
                        gpu=self.total.gpu - u.gpu)

    def can_fit(self, need: Resource) -> bool:
        return need.fits_in(self.free())

    def allocate(self, task_id: str, need: Resource, ts: float) -> None:
        """调用方已通过 can_fit; 同任务重复记账被拒绝 (防双重占用)。"""
        if task_id in self.allocations:
            raise AssertionError(f"duplicate allocation on {self.machine_id} "
                                 f"for task {task_id}")
        self.allocations[task_id] = Allocation(
            machine_id=self.machine_id, cpu=need.cpu, mem=need.mem,
            gpu=need.gpu, allocated_ts=ts)

    def release(self, task_id: str) -> Allocation | None:
        """归还某任务占用; 幂等: 重复/不存在返回 None, 绝不二次扣减。"""
        return self.allocations.pop(task_id, None)

    # ------------------------------------------------------------ snapshot
    def to_dict(self) -> dict:
        u = self.used()
        f = self.free()
        return {
            "machine_id": self.machine_id,
            "state": self.state,
            "labels": dict(self.labels),
            "created_ts": self.created_ts,
            "total": self.total.to_dict(),
            "used": u.to_dict(),
            "free": f.to_dict(),
            "allocations": [a.to_dict() for a in self.allocations.values()],
            "timeline": list(self.timeline[-TIMELINE_LIMIT:]),
        }


@dataclass
class Task:
    task_id: str
    tenant_id: str
    business_key: str
    # 内容指纹: 同 business_key 再次提交必须与之一致, 否则稳定冲突
    content_digest: str
    machine_count: int
    need: Resource                      # 每台机器需要的资源
    selector: dict[str, str]           # 机器标签选择器 (全部匹配)
    enqueued_ts: float
    deadline_ts: float | None = None
    state: str = QUEUED
    enqueue_seq: int = 0
    started_ts: float | None = None
    finished_ts: float | None = None
    # machine_id -> Allocation
    allocations: dict[str, Allocation] = field(default_factory=dict)
    # 容量变化与生命周期审计 (页面展示分配去向/容量变化/拒绝缘由)
    timeline: list[dict] = field(default_factory=list)
    # 最近一次调度推迟原因: {"code", "detail", "pass_id", "ts"}
    last_defer: dict | None = None
    last_scheduled_pass: str | None = None
    result: dict | None = None
    finish_reason: str | None = None

    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def add_timeline(self, entry: dict) -> None:
        self.timeline.append(entry)
        if len(self.timeline) > TIMELINE_LIMIT:
            del self.timeline[:-TIMELINE_LIMIT]

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "business_key": self.business_key,
            "content_digest": self.content_digest,
            "state": self.state,
            "machine_count": self.machine_count,
            "need": self.need.to_dict(),
            "selector": dict(self.selector),
            "enqueue_seq": self.enqueue_seq,
            "enqueued_ts": self.enqueued_ts,
            "deadline_ts": self.deadline_ts,
            "started_ts": self.started_ts,
            "finished_ts": self.finished_ts,
            "wait_seconds": (
                (self.started_ts or time.time()) - self.enqueued_ts
                if self.state == QUEUED or self.started_ts
                else None),
            "allocations": [a.to_dict()
                            for a in sorted(self.allocations.values(),
                                            key=lambda a: a.machine_id)],
            "timeline": list(self.timeline),
            "last_defer": dict(self.last_defer) if self.last_defer else None,
            "last_scheduled_pass": self.last_scheduled_pass,
            "result": dict(self.result) if self.result else None,
            "finish_reason": self.finish_reason,
        }
