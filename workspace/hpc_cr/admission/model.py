"""接纳器领域模型: 租户、异构机器、成组任务、影子成组分配。

状态机
------

任务 (Task):

    QUEUED ──成组容量齐备──▶ ALLOCATED ──start──▶ RUNNING
       │                         │                    │
       │ cancel                  │ terminate          │ succeed
       ▼                         ▼                    ▼
    CANCELLED ◀────────── TERMINATED (终止即归还全部容量)
                                                  SUCCEEDED (容量已归还)

    QUEUED ──提交时即判定永不可满足──▶ REJECTED (终态, 带拒绝缘由)

* ``ALLOCATED`` 表示容量已原子取得、等待用户启动; ``start`` 是幂等标记。
* 一项任务所需容量 **要么全部取得, 要么一台机器都不占用** ——
  :func:`plan_gang_placement` 先在影子视图上模拟, 任一副本放不下即整体放弃,
  绝不留下部分占用, 也不会挡住此刻可以成组启动的其它租户任务
  (调度循环跳过它继续扫描后面的任务)。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from ..errors import ValidationError

# ---------------------------------------------------------------- 任务状态
QUEUED = "QUEUED"
ALLOCATED = "ALLOCATED"          # 容量已原子取得, 等待启动
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
TERMINATED = "TERMINATED"        # 被终止, 容量已归还
CANCELLED = "CANCELLED"          # 排队中撤回
REJECTED = "REJECTED"            # 提交时即判定永不可满足

ACTIVE_TASK_STATES = (QUEUED, ALLOCATED, RUNNING)
# 已取得容量、占用机器的状态
RUNNABLE_TASK_STATES = (ALLOCATED, RUNNING)
TERMINAL_TASK_STATES = (SUCCEEDED, TERMINATED, CANCELLED, REJECTED)

# ---------------------------------------------------------------- 机器状态
ONLINE = "ONLINE"
DRAINED = "DRAINED"              # 不再接受新分配, 现有占用保留
OFFLINE = "OFFLINE"             # 强制下线前必须已无占用

# ---------------------------------------------------------------- 拒绝/暂缓缘由 (稳定机器可读码)
# 提交期永久拒绝
REJ_SINGLE_MACHINE_TOO_SMALL = "single_machine_too_small"
REJ_CLUSTER_INSUFFICIENT = "cluster_insufficient_capacity"
REJ_BAD_REQUEST = "bad_request"

# 调度期暂缓 (仍在排队, 每次调度重新评估, 不做部分占用)
DEFER_NO_FREE_MACHINE = "no_machine_matching_replica"
DEFER_CLUSTER_BUSY = "insufficient_free_capacity_cluster_busy"
DEFER_DRAINED_POOL = "no_online_machines"

ACCELERATOR_KEYS = ("gpu", "accelerators")


# ===================================================================== 租户
@dataclass
class Tenant:
    tenant_id: str
    weight: float = 1.0
    name: str | None = None
    created_ts: float = 0.0
    updated_ts: float = 0.0
    # 接纳器记账: 已接纳 (取得容量) 的任务数与运行历史, 用于可解释公平性
    admitted: int = 0
    completed: int = 0

    def snapshot(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "weight": self.weight,
            "name": self.name or self.tenant_id,
            "created_ts": self.created_ts,
            "updated_ts": self.updated_ts,
            "admitted": self.admitted,
            "completed": self.completed,
        }


# ===================================================================== 机器
@dataclass
class Slice:
    """一项任务在一台机器上取得的一个副本容量 (分配与归还的最小单元)。"""

    slice_id: str                 # f"{task_id}#r{replica}"
    task_id: str
    tenant_id: str
    replica: int
    machine_id: str
    cpus: int
    memory_mb: int
    accelerators: dict[str, int] = field(default_factory=dict)
    allocated_seq: int = 0
    released_seq: int | None = None  # None = 仍占用
    allocated_ts: float = 0.0
    released_ts: float | None = None

    @property
    def active(self) -> bool:
        return self.released_seq is None

    def snapshot(self) -> dict:
        return {
            "slice_id": self.slice_id,
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "replica": self.replica,
            "machine_id": self.machine_id,
            "cpus": self.cpus,
            "memory_mb": self.memory_mb,
            "accelerators": dict(self.accelerators),
            "allocated_seq": self.allocated_seq,
            "released_seq": self.released_seq,
            "allocated_ts": self.allocated_ts,
            "released_ts": self.released_ts,
            "active": self.active,
        }


@dataclass
class Reservation:
    """队首成组任务的 *预留* (调度态, 不持久化, 每轮重算)。

    需求要求三件事同时成立: ①容量不全取得绝不启动/不留下部分占用;
    ②资源不够的大任务不能挡住此刻可启动的其它租户任务; ③久候任务不能
    无限期饥饿。普通 gang 调度会让"需要 N 台机器同时空出"的任务被持续
    到达的小任务永久队首阻塞。预留解决这个矛盾:

    * 当某个排队任务成为当前 **全局最高分队首** 且因集群忙放不下时,
      调度器把已能/即将满足一个副本的机器挂到它名下预留;
    * 预留 **不切分容量、不形成部分占用** —— 机器上现有任务跑完前资源
      仍属原任务; 其它 *未预留* 机器照常接纳任何租户的可启动任务;
    * 一旦某台被预留机器上的任务结束, 该机器保留给队首任务, 新来的小
      任务不能再占用它, 空出机器逐台汇集直到副本齐备并一次性原子取得
      (全有)。由此队首任务等待有上界, 不可能无限饥饿。

    预留是纯调度期状态: 重启后每轮 schedule() 按当前最高分任务重建,
    不进入 journal。
    """

    task_id: str
    machine_ids: list[str] = field(default_factory=list)
    since_ts: float = 0.0
    need_replicas: int = 0
    # 仍被他人占用、但已"许给"队首的机器: 其上任务一结束 (下轮 schedule)
    # 即转入 machine_ids (已汇集)。这些机器不预留容量、不产生部分占用。
    pending_machine_ids: list[str] = field(default_factory=list)

    def snapshot(self) -> dict:
        return {
            "task_id": self.task_id,
            "machine_ids": list(self.machine_ids),
            "pending_machine_ids": list(self.pending_machine_ids),
            "since_ts": self.since_ts,
            "need_replicas": self.need_replicas,
            "gathered": len(self.machine_ids),
            "pending": len(self.pending_machine_ids),
        }


@dataclass
class Machine:
    """异构机器: CPU/内存总量 + 按型号计数的加速器。"""

    machine_id: str
    cpus: int
    memory_mb: int
    accelerators: dict[str, int] = field(default_factory=dict)
    state: str = ONLINE
    created_ts: float = 0.0
    updated_ts: float = 0.0
    drained_ts: float | None = None
    offline_ts: float | None = None

    def total_snapshot(self) -> dict:
        return {
            "machine_id": self.machine_id,
            "cpus": self.cpus,
            "memory_mb": self.memory_mb,
            "accelerators": dict(self.accelerators),
            "state": self.state,
            "created_ts": self.created_ts,
            "updated_ts": self.updated_ts,
            "drained_ts": self.drained_ts,
            "offline_ts": self.offline_ts,
        }


# ===================================================================== 任务
@dataclass
class Task:
    task_id: str
    business_key: str
    tenant_id: str
    name: str
    replicas: int
    cpus_per_replica: int
    memory_mb_per_replica: int
    accelerators_per_replica: dict[str, int]
    exclusive: bool
    content_digest: str
    spec: dict
    state: str = QUEUED
    enqueue_seq: int = 0
    enqueued_ts: float = 0.0
    allocated_seq: int | None = None
    allocated_ts: float | None = None
    started_ts: float | None = None
    finished_ts: float | None = None
    terminal_seq: int | None = None
    reject_reason: str | None = None
    reject_detail: dict | None = None
    result: dict | None = None
    slices: dict[str, Slice] = field(default_factory=dict)  # slice_id -> Slice
    # 最近一次调度暂缓缘由 (内存态, 重启后在下个调度周期重建)
    last_defer: dict | None = None
    # 历次调度暂缓计数 (容量变化可审计)
    defer_count: int = 0
    # 容量/状态变化时间线 (环形截断, 仅用于页面展示, 权威状态以事件为准)
    timeline: list[dict] = field(default_factory=list)

    def allocation(self) -> list[Slice]:
        return [s for s in self.slices.values()
                if s.allocated_seq == self.allocated_seq]

    def active_slices(self) -> list[Slice]:
        return [s for s in self.slices.values() if s.active]

    def add_timeline(self, kind: str, ts: float, detail: dict | None = None,
                     *, limit: int = 200) -> None:
        self.timeline.append({"kind": kind, "ts": ts,
                              "detail": detail or {}})
        if len(self.timeline) > limit:
            del self.timeline[:-limit]

    def snapshot(self) -> dict:
        active = self.active_slices()
        return {
            "task_id": self.task_id,
            "business_key": self.business_key,
            "tenant_id": self.tenant_id,
            "name": self.name,
            "state": self.state,
            "replicas": self.replicas,
            "cpus_per_replica": self.cpus_per_replica,
            "memory_mb_per_replica": self.memory_mb_per_replica,
            "accelerators_per_replica": dict(self.accelerators_per_replica),
            "exclusive": self.exclusive,
            "content_digest": self.content_digest,
            "spec": self.spec,
            "enqueue_seq": self.enqueue_seq,
            "enqueued_ts": self.enqueued_ts,
            "allocated_seq": self.allocated_seq,
            "allocated_ts": self.allocated_ts,
            "started_ts": self.started_ts,
            "finished_ts": self.finished_ts,
            "terminal_seq": self.terminal_seq,
            "reject_reason": self.reject_reason,
            "reject_detail": self.reject_detail,
            "result": self.result,
            "slices": [s.snapshot() for s in sorted(
                self.slices.values(), key=lambda s: s.replica)],
            "machines": sorted({s.machine_id for s in active}),
            "allocated_cpus": sum(s.cpus for s in active),
            "allocated_memory_mb": sum(s.memory_mb for s in active),
            "last_defer": self.last_defer,
            "defer_count": self.defer_count,
            "timeline": list(self.timeline),
        }


# =========================================================== 容量视图/分配器
class CapacityView:
    """机器容量视图: 按 *仍未归还* 的 slice 计算已用/可用。

    重放后状态完全由 (machine 注册事件 + slice 分配/释放事件) 重建,
    不持久化任何派生用量。
    """

    def __init__(self, machines: dict[str, Machine],
                 slices: dict[str, Slice], lock: threading.RLock | None = None):
        self._machines = machines
        self._slices = slices
        self._lock = lock or threading.RLock()

    def _active_slices_on(self, machine_id: str) -> list[Slice]:
        return [s for s in self._slices.values()
                if s.machine_id == machine_id and s.active]

    def used(self, machine_id: str) -> dict:
        sl = self._active_slices_on(machine_id)
        used_acc: dict[str, int] = {}
        for s in sl:
            for k, v in s.accelerators.items():
                used_acc[k] = used_acc.get(k, 0) + v
        return {"cpus": sum(s.cpus for s in sl),
                "memory_mb": sum(s.memory_mb for s in sl),
                "accelerators": used_acc,
                "slices": len(sl)}

    def machine_snapshot(self, m: Machine) -> dict:
        used = self.used(m.machine_id)
        acc_total = dict(m.accelerators)
        acc_used = used["accelerators"]
        acc_free = {k: acc_total.get(k, 0) - acc_used.get(k, 0)
                    for k in acc_total}
        # 已占用但机器当前不再声明的加速器型号 (例如缩容后), 如实展示为负余?
        # 不允许在线缩容到小于已占用 (API 层拒绝), 故 free 恒 >= 0。
        return {
            **m.total_snapshot(),
            "used_cpus": used["cpus"],
            "free_cpus": m.cpus - used["cpus"],
            "used_memory_mb": used["memory_mb"],
            "free_memory_mb": m.memory_mb - used["memory_mb"],
            "accelerators_total": acc_total,
            "accelerators_used": acc_used,
            "accelerators_free": acc_free,
            "active_slices": used["slices"],
            "allocations": [
                {"slice_id": s.slice_id, "task_id": s.task_id,
                 "tenant_id": s.tenant_id, "replica": s.replica,
                 "cpus": s.cpus, "memory_mb": s.memory_mb,
                 "accelerators": dict(s.accelerators)}
                for s in sorted(self._active_slices_on(m.machine_id),
                                key=lambda s: (s.task_id, s.replica))
            ],
        }

    def snapshots(self) -> list[dict]:
        with self._lock:
            return [self.machine_snapshot(self._machines[mid])
                    for mid in sorted(self._machines)]

    def free_totals(self) -> dict:
        """全集群 (仅 ONLINE) 可分配总量, 供提交期可行性判定。"""
        cpus = mem = 0
        acc: dict[str, int] = {}
        for mid, m in self._machines.items():
            if m.state != ONLINE:
                continue
            u = self.used(mid)
            cpus += m.cpus - u["cpus"]
            mem += m.memory_mb - u["memory_mb"]
            for k, total in m.accelerators.items():
                acc[k] = acc.get(k, 0) + total - u["accelerators"].get(k, 0)
        return {"cpus": cpus, "memory_mb": mem, "accelerators": acc}


def normalize_accelerators(value) -> dict[str, int]:
    if value in (None, {}, []):
        return {}
    if not isinstance(value, dict):
        raise ValidationError("accelerators must map model name to count")
    out: dict[str, int] = {}
    for model, count in value.items():
        if not isinstance(model, str) or not model.strip():
            raise ValidationError("accelerator model must be a non-empty string")
        ci = int(count)
        if ci < 0:
            raise ValidationError("accelerator count must be non-negative")
        if ci:
            out[model] = ci
    return out


def validate_demand(*, replicas: int, cpus: int, memory_mb: int,
                    accelerators: dict[str, int],
                    exclusive: bool) -> None:
    if not isinstance(replicas, int) or replicas <= 0:
        raise ValidationError("replicas must be a positive integer")
    if not isinstance(cpus, int) or cpus <= 0:
        raise ValidationError("cpus_per_replica must be a positive integer")
    if not isinstance(memory_mb, int) or memory_mb <= 0:
        raise ValidationError(
            "memory_mb_per_replica must be a positive integer")
    if int(cpus) != cpus or int(memory_mb) != memory_mb:
        raise ValidationError("capacity values must be integers")
    if not isinstance(exclusive, bool):
        raise ValidationError("exclusive must be boolean")
    normalize_accelerators(accelerators)


# ----------------------------------------------------- 影子成组分配 (纯函数)
@dataclass
class _Shadow:
    """一台机器在本轮模拟中的临时已扣减量。"""

    cpus: int
    memory_mb: int
    accelerators: dict[str, int]
    occupied: bool = False     # 本轮已被某个副本独占占位


def _fits(machine_snap: dict, shadow: _Shadow, task: Task) -> bool:
    """副本需求能否放进 (机器真实余量 - 本轮已扣减)。"""
    free_cpus = machine_snap["free_cpus"] - shadow.cpus
    free_mem = machine_snap["free_memory_mb"] - shadow.memory_mb
    if task.cpus_per_replica > free_cpus:
        return False
    if task.memory_mb_per_replica > free_mem:
        return False
    if task.exclusive and (machine_snap["active_slices"] > 0
                           or shadow.occupied):
        return False
    for model, need in task.accelerators_per_replica.items():
        total = machine_snap["accelerators_total"].get(model, 0)
        used = machine_snap["accelerators_used"].get(model, 0)
        free = total - used - shadow.accelerators.get(model, 0)
        if need > free:
            return False
    return True


def _fit_key(machine_snap: dict, task: Task):
    """最佳适配排序: 剩余越少越优先 (减少碎片); 无 GPU 需求时避免浪费带卡机;
    同序按机器 id 确定决胜。"""
    spare_cpus = machine_snap["free_cpus"] - task.cpus_per_replica
    spare_mem = machine_snap["free_memory_mb"] - task.memory_mb_per_replica
    # 不需要加速器的副本优先落在"没有该型号"或加速器更少的机器上
    acc_total = sum(machine_snap["accelerators_total"].values())
    if task.accelerators_per_replica:
        # 需要某型号: 优先该型号刚好够用 (剩余该型号最少) 的机器
        model = next(iter(task.accelerators_per_replica))
        spare_acc = (machine_snap["accelerators_total"].get(model, 0)
                     - machine_snap["accelerators_used"].get(model, 0)
                     - task.accelerators_per_replica[model])
        return (spare_acc, spare_cpus, spare_mem, machine_snap["machine_id"])
    return (acc_total, spare_cpus, spare_mem, machine_snap["machine_id"])


def plan_gang_placement(task: Task, machine_snaps: list[dict],
                        reserved_machines: set[str] | None = None) -> dict:
    """在给定机器快照上为全部副本 *原子* 规划落点。

    ``reserved_machines`` 中的机器视为对本任务不可用 (它们被调度器预留给
    其它更高队首的成组任务, 见 manager 的 gang reservation)。

    返回 ``{"fits": bool, "placements": [(replica, machine_id)...],
    "reason": ..., "shortage": {...}}``。任一副本放不下即整体失败,
    不返回任何已模拟的扣减 (调用方据此完全不占用)。同一轮内前序副本的
    扣减会影响后续副本, 因此一个任务可以在同一台机器上取得多个副本,
    但 exclusive 任务每个副本独占一台机器。
    """
    reserved_machines = reserved_machines or set()
    shadows: dict[str, _Shadow] = {
        m["machine_id"]: _Shadow(
            cpus=0, memory_mb=0,
            accelerators={k: 0 for k in m["accelerators_total"]})
        for m in machine_snaps
    }
    online = [m for m in machine_snaps
              if m["state"] == ONLINE and m["machine_id"]
              not in reserved_machines]
    placements: list[tuple[int, str]] = []
    if not online:
        return {"fits": False, "placements": [],
                "reason": DEFER_DRAINED_POOL,
                "shortage": {"online_machines": 0}}
    for replica in range(task.replicas):
        candidates = [m for m in online
                      if _fits(m, shadows[m["machine_id"]], task)]
        if not candidates:
            # 区分"有在线机器但此刻不够"与"根本没有匹配型号的机器"
            reason, shortage = _shortage_explanation(task, online, shadows)
            return {"fits": False, "placements": [],
                    "reason": reason, "shortage": shortage,
                    "failed_replica": replica}
        chosen = min(candidates, key=lambda m: _fit_key(m, task))
        sh = shadows[chosen["machine_id"]]
        sh.cpus += task.cpus_per_replica
        sh.memory_mb += task.memory_mb_per_replica
        for k, v in task.accelerators_per_replica.items():
            sh.accelerators[k] = sh.accelerators.get(k, 0) + v
        sh.occupied = True
        placements.append((replica, chosen["machine_id"]))
    return {"fits": True, "placements": placements,
            "reason": None, "shortage": None}


def _shadow_free(machine_snap: dict, shadow: _Shadow, model: str | None) -> int:
    if model is None:
        return machine_snap["free_cpus"] - shadow.cpus
    return (machine_snap["accelerators_total"].get(model, 0)
            - machine_snap["accelerators_used"].get(model, 0)
            - shadow.accelerators.get(model, 0))


def machine_can_hold_replica(machine_snap: dict, task: Task) -> bool:
    """机器 *当前空闲余量* 能否立即容纳任务的一个副本。

    用于成组预留: 只有现在已经空出的机器才会被汇集到队首任务名下;
    正在忙的机器不预留, 因此不产生任何部分占用。
    """
    if machine_snap["state"] != ONLINE:
        return False
    if task.exclusive and machine_snap["active_slices"] > 0:
        return False
    if task.cpus_per_replica > machine_snap["free_cpus"]:
        return False
    if task.memory_mb_per_replica > machine_snap["free_memory_mb"]:
        return False
    for model, need in task.accelerators_per_replica.items():
        if need > machine_snap["accelerators_free"].get(model, 0):
            return False
    return True


# 管理器以该短名引用
_machine_holds_replica = machine_can_hold_replica


def _shortage_explanation(task: Task, online: list[dict],
                          shadows: dict[str, _Shadow]) -> tuple[str, dict]:
    """失败副本的可解释缘由与缺口 (页面"拒绝缘由/容量变化"展示)。"""
    acc_model = next(iter(task.accelerators_per_replica), None)
    best = None
    for m in online:
        sh = shadows[m["machine_id"]]
        if task.exclusive and (m["active_slices"] > 0 or sh.occupied):
            continue
        free_cpu = m["free_cpus"] - sh.cpus
        free_mem = m["free_memory_mb"] - sh.memory_mb
        free_acc = _shadow_free(m, sh, acc_model) if acc_model else None
        score = min(
            free_cpu // max(task.cpus_per_replica, 1),
            free_mem // max(task.memory_mb_per_replica, 1),
            (free_acc // task.accelerators_per_replica[acc_model]
             if acc_model else 10 ** 9),
        )
        if best is None or score > best[0]:
            best = (score, m["machine_id"], free_cpu, free_mem, free_acc)
    shortage = {
        "required_per_replica": {
            "cpus": task.cpus_per_replica,
            "memory_mb": task.memory_mb_per_replica,
            "accelerators": dict(task.accelerators_per_replica),
        },
        "best_candidate": (
            {"machine_id": best[1], "free_cpus": best[2],
             "free_memory_mb": best[3], "free_accelerator": best[4]}
            if best else None),
        "exclusive": task.exclusive,
    }
    # 有任何一台在线机器声明了所需加速器型号/足够大, 只是此刻忙 -> 集群忙;
    # 一台能在空机时满足的都没有 -> 永久不可满足由提交期检查负责, 这里
    # 调度期保守报"无匹配机器"。
    can_hold_any = any(
        m["cpus"] >= task.cpus_per_replica
        and m["memory_mb"] >= task.memory_mb_per_replica
        and all(m["accelerators_total"].get(k, 0) >= v
                for k, v in task.accelerators_per_replica.items())
        and (not task.exclusive)
        for m in online
    )
    if can_hold_any:
        return DEFER_CLUSTER_BUSY, shortage
    return DEFER_NO_FREE_MACHINE, shortage
