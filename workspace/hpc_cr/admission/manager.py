"""多租户成组批任务接纳器: 编排核心。

公平性与不饥饿
==============

排队任务的调度优先级为::

    score(task) = 等待秒数 × 租户权重

* 同刻抵达时权重高的租户先得分 (3:1 的权重大致得到 3:1 的次序机会);
* 等待时长线性累积, 任意排队任务的 score 都随时间无界增长, 因此无论权重
  多低、高权重租户如何持续送入新任务, 久候任务终将排到队首 —— **不会
  无限期饥饿**;
* 同一租户内部严格 FIFO (score 相同按入队 seq), 次序可解释。

成组 (gang) 与不阻塞
====================

每轮调度按 score 从高到低扫描 *全部* 租户的排队任务:

* 任务所需的 ``replicas`` 个副本容量在 :mod:`placement` 的影子视图上整体
  模拟; 全放得下才一次性提交分配事件 (全有), 任一副本放不下则整体跳过,
  **不留下任何部分占用** (全无);
* 被跳过的任务不阻挡后续任务 —— 调度循环继续扫描更低分但此刻可以成组
  启动的其它租户任务, 直到一轮内没有任何排队任务能再取得容量。

幂等/冲突/重启
==============

* 同业务键 (``business_key``) 重复提交: 内容摘要一致 -> 返回原任务及原
  结果; 内容变化 -> 409 ``business_key_conflict``。
* 撤回/终止/归还是幂等动作: 终态后重复调用返回同一终态视图
  (``idempotent_replay=True``)。
* 全部状态变更顺序化追加到独立的 admission journal; 重启后重放重建
  租户、机器、等待项与 *已分配但未归还* 的容量。
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Callable

from ..errors import Conflict, NotFound, ValidationError
from ..security import canonical_json
from .journal import AdmissionJournal
from .model import (
    ALLOCATED,
    CANCELLED,
    OFFLINE,
    ONLINE,
    QUEUED,
    DRAINED,
    REJECTED,
    REJ_CLUSTER_INSUFFICIENT,
    REJ_SINGLE_MACHINE_TOO_SMALL,
    RUNNING,
    SUCCEEDED,
    TERMINATED,
    CapacityView,
    Machine,
    Reservation,
    Slice,
    Task,
    Tenant,
    machine_can_hold_replica,
    normalize_accelerators,
    plan_gang_placement,
    validate_demand,
)

# ----------------------------------------------------------------- 事件类型
EVT_TENANT_UPSERT = "adm_tenant_upsert"
EVT_MACHINE_REGISTERED = "adm_machine_registered"
EVT_MACHINE_DRAINED = "adm_machine_drained"
EVT_MACHINE_ONLINE = "adm_machine_online"
EVT_MACHINE_OFFLINE = "adm_machine_offline"
EVT_TASK_SUBMITTED = "adm_task_submitted"
EVT_TASK_REJECTED = "adm_task_rejected"
EVT_TASK_ALLOCATED = "adm_task_allocated"
EVT_TASK_STARTED = "adm_task_started"
EVT_TASK_CANCELLED = "adm_task_cancelled"
EVT_TASK_TERMINATED = "adm_task_terminated"
EVT_TASK_SUCCEEDED = "adm_task_succeeded"
EVT_SLICE_RELEASED = "adm_slice_released"

TIMELINE_LIMIT = 200
DEFAULT_WEIGHT = 1.0


def new_task_id() -> str:
    return f"task-{uuid.uuid4().hex[:12]}"


def content_digest(spec: dict) -> str:
    """对任务 *业务内容* 做规范化哈希 (business_key 不参与, 键相同内容变即冲突)。"""
    import hashlib
    return hashlib.sha256(canonical_json(spec)).hexdigest()


class AdmissionManager:
    """接纳器聚合根。一把可重入锁保护全部内存状态; 事件在锁内追加。"""

    def __init__(self, data_dir: str | None = None,
                 *, clock: Callable[[], float] | None = None):
        self._clock = clock or time.time
        self.journal = AdmissionJournal(data_dir)
        self._lock = threading.RLock()
        self._tenants: dict[str, Tenant] = {}
        self._machines: dict[str, Machine] = {}
        self._tasks: dict[str, Task] = {}
        self._by_business_key: dict[str, str] = {}
        self._slices: dict[str, Slice] = {}
        self._recording = False
        self._last_schedule: dict | None = None
        # 成组预留: task_id -> Reservation (调度态, 重启/每轮重算)
        self._reservations: dict[str, Reservation] = {}
        self._replay()
        self._recording = True
        # 重启后立即做一轮: 找回等待项与已分配容量, 并尝试继续接纳排队项。
        self.schedule()

    # ============================================================ 内部工具
    def _now(self) -> float:
        return self._clock()

    def _emit(self, event_type: str, detail: dict, *,
              ts: float | None = None) -> dict:
        detail = dict(detail)
        detail.setdefault("ts", ts or self._now())
        if self._recording:
            return self.journal.append(event_type, detail)
        return {"seq": None, "ts": detail["ts"], "type": event_type,
                **{k: v for k, v in detail.items() if k != "ts"}}

    def _capacity(self) -> CapacityView:
        return CapacityView(self._machines, self._slices, self._lock)

    def _get_task(self, task_id: str) -> Task:
        t = self._tasks.get(task_id)
        if t is None:
            raise NotFound(f"unknown admission task {task_id}")
        return t

    def _get_tenant(self, tenant_id: str) -> Tenant:
        t = self._tenants.get(tenant_id)
        if t is None:
            raise NotFound(f"unknown tenant {tenant_id}; register it first")
        return t

    # ================================================================ 租户
    def upsert_tenant(self, tenant_id: str, *, weight: float | None = None,
                      name: str | None = None) -> dict:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValidationError("tenant_id must be a non-empty string")
        w = DEFAULT_WEIGHT if weight is None else float(weight)
        if w <= 0:
            raise ValidationError("weight must be positive")
        with self._lock:
            ts = self._now()
            existing = self._tenants.get(tenant_id)
            if existing is None:
                tenant = Tenant(tenant_id=tenant_id, weight=w,
                                name=name or tenant_id,
                                created_ts=ts, updated_ts=ts)
                self._tenants[tenant_id] = tenant
            else:
                tenant = existing
                if weight is not None:
                    tenant.weight = w
                if name is not None:
                    tenant.name = name
                tenant.updated_ts = ts
            self._emit(EVT_TENANT_UPSERT, {
                "tenant_id": tenant_id, "weight": tenant.weight,
                "name": tenant.name, "created_ts": tenant.created_ts,
            }, ts=ts)
            return tenant.snapshot()

    def list_tenants(self) -> list[dict]:
        with self._lock:
            return [self._tenants[t].snapshot()
                    for t in sorted(self._tenants)]

    def tenant_stats(self) -> list[dict]:
        """租户维度的公平性可解释视图: 权重、排队/运行数、等待时间。"""
        now = self._now()
        with self._lock:
            out = []
            for tid in sorted(self._tenants):
                tenant = self._tenants[tid]
                tasks = [t for t in self._tasks.values()
                         if t.tenant_id == tid]
                queued = [t for t in tasks if t.state == QUEUED]
                active = [t for t in tasks
                          if t.state in (ALLOCATED, RUNNING)]
                oldest = min((t.enqueued_ts for t in queued), default=None)
                out.append({
                    **tenant.snapshot(),
                    "queued": len(queued),
                    "active": len(active),
                    "oldest_wait_seconds": (now - oldest)
                    if oldest is not None else 0.0,
                    "head_score": ((now - oldest) * tenant.weight)
                    if oldest is not None else 0.0,
                })
            return out

    # ================================================================ 机器
    def register_machine(self, machine_id: str, *, cpus: int,
                         memory_mb: int,
                         accelerators: dict | None = None,
                         state: str = ONLINE) -> dict:
        if not isinstance(machine_id, str) or not machine_id.strip():
            raise ValidationError("machine_id must be a non-empty string")
        cpus, memory_mb = int(cpus), int(memory_mb)
        if cpus <= 0 or memory_mb <= 0:
            raise ValidationError("cpus and memory_mb must be positive")
        acc = normalize_accelerators(accelerators or {})
        if state not in (ONLINE, DRAINED):
            raise ValidationError("initial machine state must be ONLINE/DRAINED")
        with self._lock:
            ts = self._now()
            existing = self._machines.get(machine_id)
            if existing is not None:
                # 幂等 upsert: 仅在机器无活跃占用时允许改容量; 状态保留。
                view = self._capacity()
                snap = view.machine_snapshot(existing)
                if snap["active_slices"] > 0 and (
                        cpus < existing.cpus or memory_mb < existing.memory_mb
                        or _acc_shrunk(existing.accelerators, acc)):
                    raise Conflict(
                        f"machine {machine_id} has active allocations; "
                        "cannot shrink capacity while slices are held")
                existing.cpus = cpus
                existing.memory_mb = memory_mb
                existing.accelerators = acc
                existing.updated_ts = ts
                machine = existing
            else:
                machine = Machine(machine_id=machine_id, cpus=cpus,
                                  memory_mb=memory_mb, accelerators=acc,
                                  state=state, created_ts=ts, updated_ts=ts,
                                  drained_ts=ts if state == DRAINED else None)
                self._machines[machine_id] = machine
            self._emit(EVT_MACHINE_REGISTERED, {
                "machine": machine.total_snapshot(),
            }, ts=ts)
            result = self._capacity().machine_snapshot(machine)
            self.schedule()
            return result

    def _set_machine_state(self, machine_id: str, new_state: str,
                           event: str, *, allow_same: bool = True) -> dict:
        with self._lock:
            m = self._machines.get(machine_id)
            if m is None:
                raise NotFound(f"unknown machine {machine_id}")
            if m.state == new_state and allow_same:
                return self._capacity().machine_snapshot(m)
            ts = self._now()
            if new_state == OFFLINE:
                used = self._capacity().used(machine_id)
                if used["slices"] > 0:
                    raise Conflict(
                        f"machine {machine_id} still hosts "
                        f"{used['slices']} active slice(s); terminate or "
                        "wait for completion before taking it OFFLINE")
                m.offline_ts = ts
            if new_state == DRAINED:
                m.drained_ts = ts
            if new_state == ONLINE:
                m.drained_ts = None
            m.state = new_state
            m.updated_ts = ts
            self._emit(event, {"machine_id": machine_id, "state": new_state},
                       ts=ts)
            snap = self._capacity().machine_snapshot(m)
            self.schedule()
            return snap

    def drain_machine(self, machine_id: str) -> dict:
        return self._set_machine_state(machine_id, DRAINED,
                                      EVT_MACHINE_DRAINED)

    def online_machine(self, machine_id: str) -> dict:
        return self._set_machine_state(machine_id, ONLINE,
                                      EVT_MACHINE_ONLINE)

    def offline_machine(self, machine_id: str) -> dict:
        return self._set_machine_state(machine_id, OFFLINE,
                                      EVT_MACHINE_OFFLINE)

    def list_machines(self) -> list[dict]:
        with self._lock:
            return self._capacity().snapshots()

    # ================================================================ 提交
    def submit(
        self,
        *,
        business_key: str,
        tenant_id: str,
        replicas: int,
        cpus_per_replica: int,
        memory_mb_per_replica: int,
        accelerators_per_replica: dict | None = None,
        exclusive: bool = False,
        name: str | None = None,
        payload: dict | None = None,
        task_id: str | None = None,
    ) -> dict:
        if not isinstance(business_key, str) or not business_key.strip():
            raise ValidationError("business_key must be a non-empty string")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValidationError("tenant_id must be a non-empty string")
        acc = normalize_accelerators(accelerators_per_replica or {})
        validate_demand(replicas=replicas, cpus=cpus_per_replica,
                        memory_mb=memory_mb_per_replica,
                        accelerators=acc, exclusive=bool(exclusive))
        payload = payload if isinstance(payload, dict) else {}
        # 业务内容: 需求规格 + 业务载荷; tenant/business_key 不参与内容摘要。
        spec = {
            "replicas": replicas,
            "cpus_per_replica": cpus_per_replica,
            "memory_mb_per_replica": memory_mb_per_replica,
            "accelerators_per_replica": acc,
            "exclusive": bool(exclusive),
            "name": name or business_key,
            "payload": payload,
        }
        digest = content_digest(spec)
        with self._lock:
            # ---- 业务键幂等/冲突
            existing_id = self._by_business_key.get(business_key)
            if existing_id is not None:
                existing = self._tasks[existing_id]
                if existing.content_digest != digest:
                    raise Conflict(
                        f"business_key {business_key!r} already refers to "
                        f"task {existing_id} with different content",
                        reason="business_key_conflict")
                return {**existing.snapshot(), "idempotent_replay": True}

            tenant = self._tenants.get(tenant_id)
            if tenant is None:
                # 自动登记默认权重租户 (提交即租户), 并解释在 timeline 里。
                ts0 = self._now()
                tenant = Tenant(tenant_id=tenant_id, weight=DEFAULT_WEIGHT,
                                name=tenant_id, created_ts=ts0, updated_ts=ts0)
                self._tenants[tenant_id] = tenant
                self._emit(EVT_TENANT_UPSERT, {
                    "tenant_id": tenant_id, "weight": tenant.weight,
                    "name": tenant.name, "created_ts": ts0,
                    "auto": True}, ts=ts0)

            ts = self._now()
            task = Task(
                task_id=task_id or new_task_id(),
                business_key=business_key,
                tenant_id=tenant_id,
                name=name or business_key,
                replicas=replicas,
                cpus_per_replica=cpus_per_replica,
                memory_mb_per_replica=memory_mb_per_replica,
                accelerators_per_replica=acc,
                exclusive=bool(exclusive),
                content_digest=digest,
                spec=spec,
                enqueue_seq=self.journal.last_seq,
                enqueued_ts=ts,
            )
            if task.task_id in self._tasks:
                raise Conflict(f"task id {task.task_id} already exists")

            # ---- 提交期永久可行性判定 (仅检查"最终是否可能", 不占用)
            infeasible = self._submission_infeasible(task)
            if infeasible is not None:
                reason, detail = infeasible
                task.state = REJECTED
                task.reject_reason = reason
                task.reject_detail = detail
                task.finished_ts = ts
                task.terminal_seq = self.journal.last_seq + 1
                self._tasks[task.task_id] = task
                self._by_business_key[business_key] = task.task_id
                task.add_timeline("rejected", ts,
                                  {"reason": reason, "detail": detail})
                self._emit(EVT_TASK_REJECTED, {
                    "task": self._persist_task(task),
                    "reason": reason, "detail": detail}, ts=ts)
                return {**task.snapshot(), "rejected": True}

            self._tasks[task.task_id] = task
            self._by_business_key[business_key] = task.task_id
            task.add_timeline("submitted", ts)
            self._emit(EVT_TASK_SUBMITTED, {
                "task": self._persist_task(task)}, ts=ts)
            self.schedule()
            return self._tasks[task.task_id].snapshot()

    def _submission_infeasible(self, task: Task):
        """返回 (reason, detail) 表示即使整个集群空闲也永不可满足。

        独占只要求"副本与机器一一对应", 因此机器 *物理上* 能放下一个副本
        即视为可容纳 —— 当前机器上已有别的任务只是运行时占用 (调度期等待
        即可), 不属于永久不可行, 不能在此拒绝。
        """
        machines = [self._capacity().machine_snapshot(m)
                    for m in self._machines.values()
                    if m.state != OFFLINE]
        if not machines:
            return None  # 空集群: 等待机器登记, 不永久拒绝
        # (1) 单副本需求是否存在一台机器在 *物理总量* 上可容纳
        holds_any = any(
            m["cpus"] >= task.cpus_per_replica
            and m["memory_mb"] >= task.memory_mb_per_replica
            and all(m["accelerators_total"].get(k, 0) >= v
                    for k, v in task.accelerators_per_replica.items())
            for m in machines
        )
        if not holds_any:
            biggest = max(machines,
                          key=lambda m: (m["cpus"], m["memory_mb"]))
            return REJ_SINGLE_MACHINE_TOO_SMALL, {
                "required_per_replica": {
                    "cpus": task.cpus_per_replica,
                    "memory_mb": task.memory_mb_per_replica,
                    "accelerators": dict(task.accelerators_per_replica),
                },
                "exclusive": task.exclusive,
                "biggest_machine": {
                    "machine_id": biggest["machine_id"],
                    "cpus": biggest["cpus"],
                    "memory_mb": biggest["memory_mb"],
                    "accelerators": biggest["accelerators_total"],
                },
            }
        # (2) 全部机器空闲时最多能放几个副本?
        max_by_cpu = max_by_mem = 0
        max_by_acc: dict[str, int] = {}
        capable_machines = 0
        for m in machines:
            if not (m["cpus"] >= task.cpus_per_replica
                    and m["memory_mb"] >= task.memory_mb_per_replica
                    and all(m["accelerators_total"].get(k, 0) >= v
                            for k, v in
                            task.accelerators_per_replica.items())):
                continue
            capable_machines += 1
            # 非独占: 同一台机器可装多个副本; 独占: 一台最多一个
            if task.exclusive:
                max_by_cpu += 1
                max_by_mem += 1
                for k in task.accelerators_per_replica:
                    max_by_acc[k] = max_by_acc.get(k, 0) + 1
            else:
                max_by_cpu += m["cpus"] // task.cpus_per_replica
                max_by_mem += m["memory_mb"] // task.memory_mb_per_replica
                for k, need in task.accelerators_per_replica.items():
                    max_by_acc[k] = max_by_acc.get(k, 0) + \
                        m["accelerators_total"].get(k, 0) // need
        max_replicas = min(
            [max_by_cpu, max_by_mem]
            + [max_by_acc[k] for k in task.accelerators_per_replica])
        if max_replicas < task.replicas:
            return REJ_CLUSTER_INSUFFICIENT, {
                "requested_replicas": task.replicas,
                "cluster_max_replicas": max_replicas,
                "capable_machines": capable_machines,
                "limits": {
                    "by_cpus": max_by_cpu,
                    "by_memory": max_by_mem,
                    "by_accelerators": (max_by_acc
                                        if max_by_acc else None),
                    "exclusive": task.exclusive,
                },
            }
        return None

    # ============================================================ 调度核心
    def schedule(self, *, now: float | None = None) -> dict:
        """运行一轮接纳调度, 返回本轮可解释报告。

        算法 (单临界区, 每个任务全有或全无):

        1. 排队任务按 ``score = 等待秒数 × 权重`` 降序;
        2. 先识别 **全局最高分队首**; 若它因集群忙放不下, 维护一个成组
           *预留*: 把"当前确实空闲、能放一个副本"的机器挂到它名下,
           并在本轮从其它任务的可用集合中剔除;
        3. 预留 **不切容量、不产生部分占用** —— 机器空闲只是被"留给"
           队首, 未预留机器照常接纳任何租户的可启动任务 (大任务不挡人);
        4. 队首一旦在"未被别人预留"的机器上能整组放下, 立即一次性原子
           取得; 若仍放不下, 它的预留刷新为 *此刻真实空闲* 的机器集合
           (之前预留但又被占的机器自动退出, 预留只增信不虚构);
        5. 被预留机器随旧任务完成逐台空出并被保留给队首, 新任务无法再
           占, 机器逐台汇集直至齐备后成组取得 —— 等待有上界, 不饥饿。

        预留为纯调度态: 重启/每轮按当前最高分任务重建, 不入 journal。
        """
        with self._lock:
            ts = now if now is not None else self._now()
            admitted, deferred = [], []
            queued = [t for t in self._tasks.values() if t.state == QUEUED]
            if not queued:
                self._reservations.clear()
            else:
                order = self._order(queued, ts)
                by_id = {t.task_id: t for t in queued}

                # 预留持有者一旦确立就稳定: 不随每轮最高分漂移而丢弃已汇集
                # 的机器 (否则刚预留的机器会被轮次间的其它任务抢走, 退化成
                # 永久队首阻塞)。持有者消失 (被接纳/撤回) 时才切换给当前
                # 最高分排队任务。
                holder_id = next(iter(self._reservations), None)
                if holder_id is None or holder_id not in by_id \
                        or by_id[holder_id].state != QUEUED:
                    holder_id = order[0].task_id
                holder = by_id[holder_id]
                snaps0 = self._capacity().snapshots()
                self._refresh_top_reservation(holder, snaps0, ts)
                gathered = set(self._reservations.get(
                    holder_id, Reservation(holder_id)).machine_ids)

                for task in order:
                    if task.state != QUEUED:
                        continue
                    snaps = self._capacity().snapshots()
                    # 持有者可以使用自己汇集的机器; 其它任务一律避开。
                    blocked = set() if task.task_id == holder_id \
                        else set(gathered)
                    plan = plan_gang_placement(task, snaps, blocked)
                    if plan["fits"]:
                        self._commit_allocation(task, plan["placements"], ts)
                        if task.task_id == holder_id:
                            self._reservations.pop(holder_id, None)
                            gathered = set()
                        admitted.append({
                            "task_id": task.task_id,
                            "tenant_id": task.tenant_id,
                            "business_key": task.business_key,
                            "replicas": task.replicas,
                            "placement": [
                                {"replica": r, "machine_id": mid}
                                for r, mid in plan["placements"]],
                            "seq": task.allocated_seq,
                        })
                        if now is None:
                            ts = self._now()
                        continue
                    if task.task_id == holder_id:
                        self._record_defer(task, plan, ts, deferred,
                                          reserved=True)
                    else:
                        self._record_defer(task, plan, ts, deferred,
                                          reserved=False)

            report = {
                "ts": ts,
                "admitted": admitted,
                "deferred": deferred,
                "queued_remaining": sum(
                    1 for t in self._tasks.values() if t.state == QUEUED),
                "reservations": [r.snapshot()
                                 for r in self._reservations.values()],
                "journal_seq": self.journal.last_seq,
            }
            self._last_schedule = report
            return report

    def _refresh_top_reservation(self, top: Task, snaps: list[dict],
                                 ts: float) -> None:
        """维护队首任务的成组预留 (双集合: 已汇集 / 待释放)。

        每轮基于 *真实* 机器快照重算, 因此预留状态永远与实际容量一致:

        * 上轮已汇集、本轮仍确实空闲可放一副本的机器 → 保持已汇集;
        * 上轮已汇集但本轮不再空闲 (正常情况下不会发生——其它任务被禁止
          使用已汇集机器; 仅在重启后预留重算的首轮可能短暂出现) → 退回
          待释放, 绝不虚构容量;
        * 待释放机器一旦空出且能放一副本 → 转入已汇集;
        * 尚未纳入预留、当前已空闲可放一副本的机器 → 新汇集;
        * 其余在线机器 (忙、且型号/容量可容纳副本) → 记为待释放, 表达
          "空出即归队首" 的有界等待意图; 不预留其容量。
        * OFFLINE 机器从两个集合移除。
        """
        res = self._reservations.get(top.task_id)
        if res is None:
            res = Reservation(task_id=top.task_id, since_ts=ts,
                              need_replicas=top.replicas)
            self._reservations[top.task_id] = res
        res.need_replicas = top.replicas

        by_id = {m["machine_id"]: m for m in snaps}
        prev_gathered = set(res.machine_ids)
        prev_pending = set(res.pending_machine_ids)

        gathered: list[str] = []
        pending: list[str] = []
        for mid, m in by_id.items():
            if m["state"] == OFFLINE:
                continue
            can_hold = machine_can_hold_replica(m, top)
            if mid in prev_gathered:
                # 已汇集名额: 仍空闲则保持; 否则退回待释放 (不虚构)
                (gathered if can_hold else pending).append(mid)
            elif can_hold and m["state"] == ONLINE:
                # 当前确实空闲: 无论之前是待释放还是未纳入, 立即汇集
                gathered.append(mid)
            elif self._machine_ever_holds(m, top):
                # 现在忙, 但该机器物理上能容纳副本: 待其释放
                pending.append(mid)
        res.machine_ids = sorted(gathered)
        res.pending_machine_ids = sorted(pending)
        if not res.machine_ids and not res.pending_machine_ids:
            self._reservations.pop(top.task_id, None)

    @staticmethod
    def _machine_ever_holds(machine_snap: dict, task: Task) -> bool:
        """机器 *物理总量* (忽略当前占用) 是否可能容纳一个副本。"""
        if machine_snap["state"] == OFFLINE:
            return False
        m = machine_snap
        if m["cpus"] < task.cpus_per_replica:
            return False
        if m["memory_mb"] < task.memory_mb_per_replica:
            return False
        if task.exclusive:
            return True  # 独占: 等机器完全空出即可
        for model, need in task.accelerators_per_replica.items():
            if m["accelerators_total"].get(model, 0) < need:
                return False
        return True

    def _record_defer(self, task: Task, plan: dict, ts: float,
                      deferred: list, *, reserved: bool) -> None:
        task.defer_count += 1
        entry = {
            "ts": ts, "reason": plan["reason"],
            "shortage": plan.get("shortage"),
            "failed_replica": plan.get("failed_replica"),
        }
        if reserved and task.task_id in self._reservations:
            entry["reservation"] = \
                self._reservations[task.task_id].snapshot()
        task.last_defer = entry
        task.add_timeline("deferred", ts, {
            "reason": plan["reason"],
            "shortage": plan.get("shortage"),
            "reservation": entry.get("reservation"),
        })
        if not any(d["task_id"] == task.task_id for d in deferred):
            deferred.append({
                "task_id": task.task_id,
                "tenant_id": task.tenant_id,
                "business_key": task.business_key,
                "reason": plan["reason"],
                "shortage": plan.get("shortage"),
                "score": self._score(task, ts),
                "reservation": entry.get("reservation"),
            })

    def _order(self, queued: list[Task], ts: float) -> list[Task]:
        """score = wait * weight; 同分时同租户/全局按入队 seq (FIFO 决胜)。"""
        def key(t: Task):
            tenant = self._tenants.get(t.tenant_id)
            weight = tenant.weight if tenant is not None else DEFAULT_WEIGHT
            return (-(ts - t.enqueued_ts) * weight, t.enqueue_seq)
        return sorted(queued, key=key)

    def _score(self, t: Task, ts: float) -> float:
        tenant = self._tenants.get(t.tenant_id)
        w = tenant.weight if tenant is not None else DEFAULT_WEIGHT
        return (ts - t.enqueued_ts) * w

    def _commit_allocation(self, task: Task,
                           placements: list[tuple[int, str]],
                           ts: float) -> None:
        """原子提交整组分配: 一次性事件携带全部 slice, 要么全部可见。"""
        slices = []
        for replica, machine_id in placements:
            sid = f"{task.task_id}#r{replica}"
            sl = Slice(
                slice_id=sid, task_id=task.task_id,
                tenant_id=task.tenant_id, replica=replica,
                machine_id=machine_id,
                cpus=task.cpus_per_replica,
                memory_mb=task.memory_mb_per_replica,
                accelerators=dict(task.accelerators_per_replica),
                allocated_ts=ts)
            slices.append(sl)
        evt = self._emit(EVT_TASK_ALLOCATED, {
            "task_id": task.task_id,
            "tenant_id": task.tenant_id,
            "business_key": task.business_key,
            "slices": [
                {"slice_id": s.slice_id, "replica": s.replica,
                 "machine_id": s.machine_id, "cpus": s.cpus,
                 "memory_mb": s.memory_mb,
                 "accelerators": s.accelerators}
                for s in slices],
        }, ts=ts)
        seq = evt["seq"]
        for s in slices:
            s.allocated_seq = seq
            self._slices[s.slice_id] = s
            task.slices[s.slice_id] = s
        task.state = ALLOCATED
        task.allocated_seq = seq
        task.allocated_ts = ts
        task.last_defer = None
        task.add_timeline("allocated", ts, {
            "seq": seq,
            "placement": [{"replica": r, "machine_id": m}
                          for r, m in placements]})
        tenant = self._tenants.get(task.tenant_id)
        if tenant is not None:
            tenant.admitted += 1

    # ====================================================== 启动/撤回/终态
    def start_task(self, task_id: str) -> dict:
        with self._lock:
            task = self._get_task(task_id)
            if task.state == RUNNING:
                return {**task.snapshot(), "idempotent_replay": True}
            if task.state == ALLOCATED:
                ts = self._now()
                task.state = RUNNING
                task.started_ts = ts
                task.add_timeline("started", ts)
                self._emit(EVT_TASK_STARTED,
                           {"task_id": task_id}, ts=ts)
                return task.snapshot()
            if task.state == QUEUED:
                raise Conflict(
                    f"task {task_id} is still QUEUED; capacity has not been "
                    "allocated yet")
            raise Conflict(
                f"task {task_id} is {task.state}, cannot start")

    def cancel_task(self, task_id: str, *,
                    reason: str = "cancelled_by_submitter") -> dict:
        """撤回排队项 (幂等): 仅 QUEUED 可撤回; 已取得容量须用 terminate。"""
        with self._lock:
            task = self._get_task(task_id)
            if task.state == CANCELLED:
                return {**task.snapshot(), "idempotent_replay": True}
            if task.state != QUEUED:
                raise Conflict(
                    f"task {task_id} is {task.state}; only QUEUED tasks can "
                    "be withdrawn; use terminate to release held capacity")
            ts = self._now()
            task.state = CANCELLED
            task.finished_ts = ts
            task.add_timeline("cancelled", ts, {"reason": reason})
            self._emit(EVT_TASK_CANCELLED, {
                "task_id": task_id, "reason": reason}, ts=ts)
            # 若被撤回的是成组预留持有者, 释放其汇集的机器并重排
            self.schedule()
            return task.snapshot()

    def _release_slices(self, task: Task, ts: float) -> int:
        """归还任务当前仍占用的全部 slice; 已归还的幂等跳过。"""
        released = 0
        active = task.active_slices()
        for s in active:
            evt = self._emit(EVT_SLICE_RELEASED, {
                "slice_id": s.slice_id, "task_id": task.task_id,
                "machine_id": s.machine_id, "replica": s.replica,
            }, ts=ts)
            s.released_seq = evt["seq"]
            s.released_ts = ts
            released += 1
        return released

    def terminate_task(self, task_id: str, *,
                       reason: str = "terminated_by_user") -> dict:
        """终止运行/已分配项并归还资源 (幂等)。"""
        with self._lock:
            task = self._get_task(task_id)
            if task.state == TERMINATED:
                return {**task.snapshot(), "idempotent_replay": True}
            if task.state in (SUCCEEDED, CANCELLED, REJECTED):
                raise Conflict(
                    f"task {task_id} is already {task.state}")
            if task.state == QUEUED:
                raise Conflict(
                    f"task {task_id} is QUEUED (holds no capacity); "
                    "use cancel to withdraw it")
            ts = self._now()
            released = self._release_slices(task, ts)
            task.state = TERMINATED
            task.finished_ts = ts
            task.add_timeline("terminated", ts,
                              {"reason": reason, "released_slices": released})
            self._emit(EVT_TASK_TERMINATED, {
                "task_id": task_id, "reason": reason,
                "released_slices": released}, ts=ts)
            self._bump_tenant_completed(task)
            # 归还容量后, 排队项可能立刻可以成组启动
            self.schedule()
            return task.snapshot()

    def complete_task(self, task_id: str, *, result: dict | None = None,
                      status: str = "succeeded") -> dict:
        """任务运行完成并归还容量。succeeded 是正常终态 (幂等)。"""
        with self._lock:
            task = self._get_task(task_id)
            if status not in ("succeeded", "failed"):
                raise ValidationError("status must be succeeded/failed")
            terminal = SUCCEEDED if status == "succeeded" else TERMINATED
            if task.state in (SUCCEEDED, TERMINATED):
                return {**task.snapshot(), "idempotent_replay": True,
                        "already_terminal": True}
            if task.state == QUEUED:
                raise Conflict(
                    f"task {task_id} never started; cancel it instead")
            if task.state == ALLOCATED and status == "succeeded":
                raise Conflict(
                    f"task {task_id} is ALLOCATED but not RUNNING; "
                    "start it before reporting completion")
            ts = self._now()
            released = self._release_slices(task, ts)
            task.state = terminal
            task.finished_ts = ts
            task.result = result if isinstance(result, dict) else \
                ({"status": status} if result is None else {"value": result})
            task.add_timeline("completed", ts, {
                "status": status, "released_slices": released,
                "result": task.result})
            if status == "succeeded":
                self._emit(EVT_TASK_SUCCEEDED, {
                    "task_id": task_id, "result": task.result,
                    "released_slices": released}, ts=ts)
            else:
                self._emit(EVT_TASK_TERMINATED, {
                    "task_id": task_id,
                    "reason": "reported_failed",
                    "released_slices": released}, ts=ts)
            self._bump_tenant_completed(task)
            self.schedule()
            return task.snapshot()

    def _bump_tenant_completed(self, task: Task) -> None:
        tenant = self._tenants.get(task.tenant_id)
        if tenant is not None:
            tenant.completed += 1

    # ================================================================ 查询
    def get_task(self, task_id: str) -> dict:
        with self._lock:
            return self._get_task(task_id).snapshot()

    def get_task_by_key(self, business_key: str) -> dict:
        with self._lock:
            tid = self._by_business_key.get(business_key)
            if tid is None:
                raise NotFound(f"no task with business_key {business_key!r}")
            return self._tasks[tid].snapshot()

    def list_tasks(self, *, tenant_id: str | None = None,
                   state: str | None = None) -> list[dict]:
        with self._lock:
            out = []
            for t in self._tasks.values():
                if tenant_id is not None and t.tenant_id != tenant_id:
                    continue
                if state is not None and t.state != state:
                    continue
                out.append(t.snapshot())
        out.sort(key=lambda d: d["enqueue_seq"] or 0)
        return out

    def last_schedule(self) -> dict | None:
        with self._lock:
            return self._last_schedule

    def overview(self) -> dict:
        """浏览器总览页 / 聚合 JSON。"""
        with self._lock:
            now = self._now()
            tasks = list(self._tasks.values())
            by_state: dict[str, int] = {}
            for t in tasks:
                by_state[t.state] = by_state.get(t.state, 0) + 1
            queued = [t for t in tasks if t.state == QUEUED]
            queued_view = [{
                "task_id": t.task_id,
                "tenant_id": t.tenant_id,
                "business_key": t.business_key,
                "score": self._score(t, now),
                "wait_seconds": now - t.enqueued_ts,
                "weight": (self._tenants.get(t.tenant_id).weight
                           if t.tenant_id in self._tenants else DEFAULT_WEIGHT),
                "replicas": t.replicas,
                "last_defer": t.last_defer,
            } for t in self._order(queued, now)]
            return {
                "server_ts": now,
                "journal_seq": self.journal.last_seq,
                "persistent": self.journal.persistent,
                "tenants": self.tenant_stats(),
                "machines": self._capacity().snapshots(),
                "tasks_by_state": by_state,
                "queue_order": queued_view,
                "active_tasks": [
                    t.snapshot() for t in tasks
                    if t.state in (ALLOCATED, RUNNING)],
                "recent_tasks": sorted(
                    (t.snapshot() for t in tasks),
                    key=lambda d: -(d.get("terminal_seq") or
                                    d.get("allocated_seq") or
                                    d.get("enqueue_seq") or 0))[:50],
                "last_schedule": self._last_schedule,
            }

    # ============================================================ 持久化
    def close(self) -> None:
        self.journal.close()

    @staticmethod
    def _persist_task(t: Task) -> dict:
        """提交事件中的自包含任务定义 (重放据此完整重建排队项)。"""
        return {
            "task_id": t.task_id,
            "business_key": t.business_key,
            "tenant_id": t.tenant_id,
            "name": t.name,
            "replicas": t.replicas,
            "cpus_per_replica": t.cpus_per_replica,
            "memory_mb_per_replica": t.memory_mb_per_replica,
            "accelerators_per_replica": dict(t.accelerators_per_replica),
            "exclusive": t.exclusive,
            "content_digest": t.content_digest,
            "spec": t.spec,
            "enqueue_seq": t.enqueue_seq,
            "enqueued_ts": t.enqueued_ts,
        }

    def _replay(self) -> None:
        for evt in self.journal.replay():
            self._apply(evt)

    def _apply(self, evt: dict) -> None:
        etype = evt["type"]
        detail = {k: v for k, v in evt.items()
                  if k not in ("seq", "ts", "type")}
        ts = evt.get("ts") or self._now()
        seq = evt.get("seq")

        if etype == EVT_TENANT_UPSERT:
            self._tenants[detail["tenant_id"]] = Tenant(
                tenant_id=detail["tenant_id"],
                weight=float(detail["weight"]),
                name=detail.get("name"),
                created_ts=float(detail.get("created_ts") or ts),
                updated_ts=ts)
            return

        if etype == EVT_MACHINE_REGISTERED:
            md = detail["machine"]
            self._machines[md["machine_id"]] = Machine(
                machine_id=md["machine_id"], cpus=int(md["cpus"]),
                memory_mb=int(md["memory_mb"]),
                accelerators=dict(md.get("accelerators") or {}),
                state=md.get("state", ONLINE),
                created_ts=float(md.get("created_ts") or ts),
                updated_ts=ts,
                drained_ts=md.get("drained_ts"),
                offline_ts=md.get("offline_ts"))
            return
        if etype == EVT_MACHINE_DRAINED:
            m = self._machines.get(detail["machine_id"])
            if m:
                m.state, m.drained_ts, m.updated_ts = DRAINED, ts, ts
            return
        if etype == EVT_MACHINE_ONLINE:
            m = self._machines.get(detail["machine_id"])
            if m:
                m.state, m.drained_ts, m.updated_ts = ONLINE, None, ts
            return
        if etype == EVT_MACHINE_OFFLINE:
            m = self._machines.get(detail["machine_id"])
            if m:
                m.state, m.offline_ts, m.updated_ts = OFFLINE, ts, ts
            return

        if etype in (EVT_TASK_SUBMITTED, EVT_TASK_REJECTED):
            t = self._restore_task(detail["task"], ts)
            if etype == EVT_TASK_REJECTED:
                t.state = REJECTED
                t.reject_reason = detail.get("reason")
                t.reject_detail = detail.get("detail")
                t.finished_ts = ts
                t.terminal_seq = seq
                t.add_timeline("rejected", ts,
                                {"reason": t.reject_reason})
            else:
                t.add_timeline("submitted", t.enqueued_ts)
            self._tasks[t.task_id] = t
            self._by_business_key[t.business_key] = t.task_id
            return

        task = self._tasks.get(detail.get("task_id"))

        if etype == EVT_TASK_ALLOCATED:
            if task is None:
                return
            for sd in detail["slices"]:
                sid = sd["slice_id"]
                if sid in self._slices:  # 重复事件幂等
                    continue
                sl = Slice(
                    slice_id=sid, task_id=task.task_id,
                    tenant_id=task.tenant_id,
                    replica=int(sd["replica"]),
                    machine_id=sd["machine_id"],
                    cpus=int(sd["cpus"]),
                    memory_mb=int(sd["memory_mb"]),
                    accelerators=dict(sd.get("accelerators") or {}),
                    allocated_seq=seq, allocated_ts=ts)
                self._slices[sid] = sl
                task.slices[sid] = sl
            # 只接受最新一次分配为当前占用 (重跑/重放旧事件不回退状态)
            if task.allocated_seq is None or seq >= task.allocated_seq:
                task.state = ALLOCATED
                task.allocated_seq = seq
                task.allocated_ts = ts
                task.add_timeline("allocated", ts, {"seq": seq})
            tenant = self._tenants.get(task.tenant_id)
            if tenant is not None:
                tenant.admitted += 1
            return

        if task is None:
            return
        if etype == EVT_TASK_STARTED:
            task.state = RUNNING
            task.started_ts = ts
            task.add_timeline("started", ts)
        elif etype == EVT_TASK_CANCELLED:
            task.state = CANCELLED
            task.finished_ts = ts
            task.terminal_seq = seq
            task.add_timeline("cancelled", ts, {"reason": detail.get("reason")})
        elif etype == EVT_SLICE_RELEASED:
            sl = self._slices.get(detail["slice_id"])
            if sl is not None and sl.active:
                sl.released_seq = seq
                sl.released_ts = ts
        elif etype == EVT_TASK_TERMINATED:
            for sl in list(task.slices.values()):
                if sl.active:
                    sl.released_seq = seq
                    sl.released_ts = ts
            task.state = TERMINATED
            task.finished_ts = ts
            task.terminal_seq = seq
            task.add_timeline("terminated", ts,
                              {"reason": detail.get("reason")})
            tenant = self._tenants.get(task.tenant_id)
            if tenant is not None:
                tenant.completed += 1
        elif etype == EVT_TASK_SUCCEEDED:
            for sl in list(task.slices.values()):
                if sl.active:
                    sl.released_seq = seq
                    sl.released_ts = ts
            task.state = SUCCEEDED
            task.finished_ts = ts
            task.terminal_seq = seq
            task.result = detail.get("result")
            task.add_timeline("completed", ts, {"status": "succeeded"})
            tenant = self._tenants.get(task.tenant_id)
            if tenant is not None:
                tenant.completed += 1

    def _restore_task(self, d: dict, ts: float) -> Task:
        return Task(
            task_id=d["task_id"],
            business_key=d["business_key"],
            tenant_id=d["tenant_id"],
            name=d.get("name") or d["business_key"],
            replicas=int(d["replicas"]),
            cpus_per_replica=int(d["cpus_per_replica"]),
            memory_mb_per_replica=int(d["memory_mb_per_replica"]),
            accelerators_per_replica=dict(
                d.get("accelerators_per_replica") or {}),
            exclusive=bool(d.get("exclusive", False)),
            content_digest=d["content_digest"],
            spec=d.get("spec") or {},
            enqueue_seq=int(d.get("enqueue_seq") or 0),
            enqueued_ts=float(d.get("enqueued_ts") or ts),
        )


def _acc_shrunk(old: dict[str, int], new: dict[str, int]) -> bool:
    for model, count in old.items():
        if new.get(model, 0) < count:
            return True
    return False
