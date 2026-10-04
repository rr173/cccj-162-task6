"""BatchManager: 多租户批任务接纳器聚合根。

并发模型: 管理器内一把 RLock 串行所有变更; 一次调度轮次中的"检查容量 →
成组记账 → 启动"在同一临界区内原子完成, 因此:

  * 成组需求要么全部拿到机器容量 (RUNNING), 要么一台都不占 (QUEUED);
  * 不会出现部分占用, 也不会跨租户队头阻塞;
  * 撤回/终止/归还与提交并发时, 每个业务键的终态/账目恰好落定一次。

持久化: 独立事件日志 ``<data_dir>/batch/journal.jsonl`` (复用
``hpc_cr.persistence.Journal`` 的 fsync/崩溃半行语义), 与既有作业恢复
journal 完全分离, 互不影响。
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Callable

from ..persistence import Journal
from ..security import canonical_json, sha256_hex
from .errors import BatchConflict, BatchNotFound, BatchValidationError
from .model import (
    CANCELLED,
    COMPLETED,
    DRAINING,
    ENABLED,
    FAILED,
    QUEUED,
    REMOVED,
    RESOURCE_KEYS,
    RUNNING,
    TERMINATED,
    TERMINAL_STATES,
    Allocation,
    Machine,
    Resource,
    Task,
    Tenant,
)
from .scheduling import (
    cluster_feasible,
    defer_detail,
    find_placement,
    order_queued,
)

# 事件名
EV_TENANT_REGISTERED = "batch_tenant_registered"
EV_TENANT_UPDATED = "batch_tenant_updated"
EV_MACHINE_ADDED = "batch_machine_added"
EV_MACHINE_UPDATED = "batch_machine_updated"
EV_MACHINE_STATE = "batch_machine_state"
EV_TASK_SUBMITTED = "batch_task_submitted"
EV_TASK_DEFERRED = "batch_task_deferred"
EV_TASK_ALLOCATED = "batch_task_allocated"
EV_TASK_COMPLETED = "batch_task_completed"
EV_TASK_FAILED = "batch_task_failed"
EV_TASK_CANCELLED = "batch_task_cancelled"
EV_TASK_TERMINATED = "batch_task_terminated"

RECENT_PASSES = 100

DEFAULT_AGING_AFTER = 30.0     # 等候超过 30s 开始老化加权
DEFAULT_AGING_RATE = 2.0       # 每多等 1s, 得分下移 2.0 (权重单位)


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def compute_digest(machine_count: int, need: Resource,
                   selector: dict[str, str]) -> str:
    """任务内容指纹 (业务键相同但内容变化即据此检出冲突)。"""
    payload = {
        "machine_count": int(machine_count),
        "cpu": need.cpu,
        "mem": need.mem,
        "gpu": need.gpu,
        "selector": dict(selector or {}),
    }
    return "sha256:" + sha256_hex(canonical_json(payload))


class BatchManager:
    def __init__(
        self,
        data_dir: str | None = None,
        *,
        clock: Callable[[], float] | None = None,
        aging_after: float = DEFAULT_AGING_AFTER,
        aging_rate: float = DEFAULT_AGING_RATE,
        auto_tick: bool = False,
        tick_interval: float = 1.0,
    ):
        batch_dir = os.path.join(data_dir, "batch") if data_dir else None
        self.journal = Journal(batch_dir)
        self._lock = threading.RLock()
        self._clock = clock or time.time
        self.aging_after = float(aging_after)
        self.aging_rate = float(aging_rate)

        self._tenants: dict[str, Tenant] = {}
        self._machines: dict[str, Machine] = {}
        self._tasks: dict[str, Task] = {}
        # (tenant_id, business_key) -> task_id
        self._idempotency: dict[tuple[str, str], str] = {}
        self._enqueue_counter = 0
        self._passes: list[dict] = []          # 仅内存保留最近轮次
        self._recording = False
        self._replay()
        self._recording = True

        self._tick_timer: threading.Timer | None = None
        self._tick_interval = tick_interval
        if auto_tick:
            self.start_ticker()

    # ============================================================ plumbing
    def _emit(self, event_type: str, detail: dict) -> dict:
        if self._recording:
            return self.journal.append(event_type, detail)
        return {"seq": None, "ts": detail.get("ts") or self._clock(),
                "type": event_type, **detail}

    def _now(self) -> float:
        return float(self._clock())

    def close(self) -> None:
        self.stop_ticker()
        self.journal.close()

    # ----------------------------------------------------------- tenants
    def register_tenant(self, tenant_id: str, weight: float = 1.0) -> dict:
        if not tenant_id or not isinstance(tenant_id, str):
            raise BatchValidationError("tenant_id is required")
        try:
            weight = float(weight)
        except (TypeError, ValueError) as exc:
            raise BatchValidationError("weight must be a number") from exc
        if weight <= 0:
            raise BatchValidationError("weight must be positive")
        with self._lock:
            existing = self._tenants.get(tenant_id)
            if existing is not None:
                raise BatchConflict(
                    f"tenant {tenant_id} already registered",
                    reason="tenant_exists")
            ts = self._now()
            tenant = Tenant(tenant_id=tenant_id, weight=weight,
                            created_ts=ts, fair_key=0.0,
                            baseline_aligned=False)
            self._tenants[tenant_id] = tenant
            self._emit(EV_TENANT_REGISTERED, {
                "tenant_id": tenant_id, "weight": weight, "ts": ts,
                "fair_key": 0.0})
            return tenant.to_dict()

    def update_tenant(self, tenant_id: str, weight: float) -> dict:
        try:
            weight = float(weight)
        except (TypeError, ValueError) as exc:
            raise BatchValidationError("weight must be a number") from exc
        if weight <= 0:
            raise BatchValidationError("weight must be positive")
        with self._lock:
            tenant = self._require_tenant(tenant_id)
            tenant.weight = weight
            self._emit(EV_TENANT_UPDATED, {
                "tenant_id": tenant_id, "weight": weight,
                "ts": self._now()})
            return tenant.to_dict()

    def _require_tenant(self, tenant_id: str) -> Tenant:
        tenant = self._tenants.get(tenant_id)
        if tenant is None:
            raise BatchNotFound(f"tenant {tenant_id} not found",
                                reason="unknown_tenant")
        return tenant

    # ---------------------------------------------------------- machines
    def add_machine(
        self,
        machine_id: str,
        total: dict | Resource,
        *,
        labels: dict[str, str] | None = None,
    ) -> dict:
        if not machine_id or not isinstance(machine_id, str):
            raise BatchValidationError("machine_id is required")
        res = self._validate_resource(total, require_positive=True)
        labels = self._validate_labels(labels)
        with self._lock:
            if machine_id in self._machines:
                raise BatchConflict(f"machine {machine_id} already exists",
                                    reason="machine_exists")
            ts = self._now()
            machine = Machine(machine_id=machine_id, total=res, labels=labels,
                              created_ts=ts)
            machine.timeline.append({
                "ts": ts, "kind": "added",
                "total": res.to_dict(), "detail": {"labels": dict(labels)}})
            self._machines[machine_id] = machine
            self._emit(EV_MACHINE_ADDED, {
                "machine_id": machine_id, "total": res.to_dict(),
                "labels": dict(labels), "ts": ts})
            self._schedule_locked()
            return machine.to_dict()

    def update_machine(
        self,
        machine_id: str,
        *,
        total: dict | Resource | None = None,
        labels: dict[str, str] | None = None,
    ) -> dict:
        with self._lock:
            machine = self._require_machine(machine_id)
            ts = self._now()
            if total is not None:
                res = self._validate_resource(total, require_positive=True)
                used = machine.used()
                if (res.cpu < used.cpu or res.mem < used.mem
                        or res.gpu < used.gpu):
                    raise BatchConflict(
                        "cannot shrink total below current usage",
                        reason="capacity_below_usage")
                old = machine.total
                machine.total = res
                machine.timeline.append({
                    "ts": ts, "kind": "updated",
                    "total": res.to_dict(),
                    "detail": {"old_total": old.to_dict()}})
                self._emit(EV_MACHINE_UPDATED, {
                    "machine_id": machine_id, "total": res.to_dict(),
                    "old_total": old.to_dict(), "ts": ts})
            if labels is not None:
                labels = self._validate_labels(labels)
                machine.labels = labels
                machine.timeline.append({
                    "ts": ts, "kind": "labels_updated",
                    "total": machine.total.to_dict(),
                    "detail": {"labels": dict(labels)}})
                self._emit(EV_MACHINE_UPDATED, {
                    "machine_id": machine_id, "total": machine.total.to_dict(),
                    "old_total": machine.total.to_dict(),
                    "labels": dict(labels), "ts": ts})
            self._schedule_locked()
            return machine.to_dict()

    def set_machine_state(self, machine_id: str, state: str) -> dict:
        if state not in (ENABLED, DRAINING, REMOVED):
            raise BatchValidationError(
                f"state must be one of {ENABLED}/{DRAINING}/{REMOVED}")
        with self._lock:
            machine = self._require_machine(machine_id)
            if state == REMOVED and machine.allocations:
                raise BatchConflict(
                    "machine still has running allocations",
                    reason="machine_busy")
            if machine.state != state:
                ts = self._now()
                machine.state = state
                machine.timeline.append({
                    "ts": ts, "kind": "state_changed",
                    "total": machine.total.to_dict(),
                    "detail": {"state": state}})
                self._emit(EV_MACHINE_STATE, {
                    "machine_id": machine_id, "state": state, "ts": ts})
                self._schedule_locked()
            return machine.to_dict()

    def _require_machine(self, machine_id: str) -> Machine:
        machine = self._machines.get(machine_id)
        if machine is None:
            raise BatchNotFound(f"machine {machine_id} not found",
                                reason="unknown_machine")
        return machine

    @staticmethod
    def _validate_labels(labels) -> dict[str, str]:
        if labels is None:
            return {}
        if not isinstance(labels, dict) or not all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in labels.items()):
            raise BatchValidationError(
                "selector/labels must be an object of string pairs")
        return dict(labels)

    @staticmethod
    def _validate_resource(data, *, require_positive: bool) -> Resource:
        if isinstance(data, Resource):
            res = data
        elif isinstance(data, dict):
            bad = [k for k in data if k not in RESOURCE_KEYS]
            if bad:
                raise BatchValidationError(
                    f"unknown resource fields: {','.join(bad)}")
            res = Resource.from_dict(data)
        else:
            raise BatchValidationError(
                "resource must be an object {cpu, mem, gpu}")
        if res.cpu < 0 or res.mem < 0 or res.gpu < 0:
            raise BatchValidationError("resource amounts must be >= 0")
        if require_positive and not res.is_positive():
            raise BatchValidationError(
                "machine must provide at least one unit of cpu/mem/gpu")
        return res

    # ------------------------------------------------------------- submit
    def submit_task(
        self,
        tenant_id: str,
        business_key: str,
        machine_count: int,
        need: dict | Resource,
        *,
        selector: dict[str, str] | None = None,
        task_id: str | None = None,
        deadline_seconds: float | None = None,
    ) -> dict:
        if not business_key or not isinstance(business_key, str):
            raise BatchValidationError("business_key is required")
        try:
            machine_count = int(machine_count)
        except (TypeError, ValueError) as exc:
            raise BatchValidationError("machine_count must be an int") \
                from exc
        if machine_count <= 0:
            raise BatchValidationError("machine_count must be positive")
        res = self._validate_resource(need, require_positive=False)
        if not res.is_positive():
            raise BatchValidationError(
                "task must request at least one unit of cpu/mem/gpu")
        selector = self._validate_labels(selector)
        if deadline_seconds is not None:
            try:
                deadline_seconds = float(deadline_seconds)
            except (TypeError, ValueError) as exc:
                raise BatchValidationError(
                    "deadline_seconds must be a number") from exc
            if deadline_seconds <= 0:
                raise BatchValidationError(
                    "deadline_seconds must be positive")

        with self._lock:
            tenant = self._require_tenant(tenant_id)
            digest = compute_digest(machine_count, res, selector)
            idem_key = (tenant_id, business_key)
            existing_id = self._idempotency.get(idem_key)
            if existing_id is not None:
                existing = self._tasks[existing_id]
                if existing.content_digest != digest:
                    # 键相同但内容变化: 稳定冲突, 绝不覆盖原结果
                    raise BatchConflict(
                        "business_key reused with different task content",
                        reason="business_key_conflict")
                snapshot = existing.to_dict()
                snapshot["idempotent"] = True
                return snapshot

            # 接纳闸门: 全集群物理总量都不可能满足时立刻拒绝,
            # 不制造注定永远排队的等待项; 瞬时碎片不在此列。
            reject = cluster_feasible(list(self._machines.values()),
                                      _StubTask(machine_count, res, selector))
            if reject is not None:
                raise BatchConflict(
                    "task can never fit the registered cluster capacity",
                    reason=reject)

            task_id = task_id or _new_id("task")
            if task_id in self._tasks:
                raise BatchConflict(f"task {task_id} already exists",
                                    reason="task_exists")
            # 租户首次入队时, 公平基线对齐到在场租户的最大 fair_key:
            # 既不能以 0 键插到老租户前面, 也不能被压到队尾无法起步。
            if not tenant.baseline_aligned:
                tenant.fair_key = max(
                    (t.fair_key for t in self._tenants.values()
                     if t.tenant_id != tenant_id),
                    default=0.0)
                tenant.baseline_aligned = True
            ts = self._now()
            self._enqueue_counter += 1
            task = Task(
                task_id=task_id, tenant_id=tenant_id,
                business_key=business_key, content_digest=digest,
                machine_count=machine_count, need=res, selector=selector,
                enqueued_ts=ts, enqueue_seq=self._enqueue_counter,
                deadline_ts=(ts + deadline_seconds
                             if deadline_seconds else None))
            task.add_timeline({"ts": ts, "kind": "submitted",
                               "detail": {"content_digest": digest,
                                          "enqueue_seq": task.enqueue_seq}})
            self._tasks[task_id] = task
            self._idempotency[idem_key] = task_id
            self._emit(EV_TASK_SUBMITTED, {
                "task_id": task_id, "tenant_id": tenant_id,
                "business_key": business_key, "content_digest": digest,
                "machine_count": machine_count, "need": res.to_dict(),
                "selector": dict(selector), "enqueued_ts": ts,
                "enqueue_seq": task.enqueue_seq,
                "deadline_ts": task.deadline_ts})

            self._schedule_locked()
            snapshot = task.to_dict()
            snapshot["idempotent"] = False
            return snapshot

    # ------------------------------------------------- cancellations etc.
    def cancel_task(self, task_id: str, *,
                    reason: str = "cancelled_by_user") -> dict:
        """撤回排队项。仅 QUEUED 可撤回; 对已撤回重复调用幂等。"""
        with self._lock:
            task = self._require_task(task_id)
            if task.state == CANCELLED:
                return {"idempotent": True, **task.to_dict()}
            if task.state != QUEUED:
                raise BatchConflict(
                    f"task in state {task.state} cannot be cancelled",
                    reason="not_cancellable")
            self._cancel_locked(task, reason)
            self._schedule_locked()
            return {"idempotent": False, **task.to_dict()}

    def _cancel_locked(self, task: Task, reason: str) -> None:
        ts = self._now()
        task.state = CANCELLED
        task.finished_ts = ts
        task.finish_reason = reason
        task.add_timeline({"ts": ts, "kind": "cancelled",
                           "detail": {"reason": reason}})
        self._emit(EV_TASK_CANCELLED, {
            "task_id": task.task_id, "tenant_id": task.tenant_id,
            "reason": reason, "ts": ts})

    def terminate_task(self, task_id: str, *,
                       reason: str = "terminated_by_user") -> dict:
        """终止运行项并归还全部容量; 对已终止重复调用幂等。"""
        with self._lock:
            task = self._require_task(task_id)
            if task.state == TERMINATED:
                return {"idempotent": True, **task.to_dict()}
            if task.state != RUNNING:
                raise BatchConflict(
                    f"task in state {task.state} cannot be terminated",
                    reason="not_terminatable")
            self._terminate_locked(task, reason)
            self._schedule_locked()
            return {"idempotent": False, **task.to_dict()}

    def release_resources(self, task_id: str) -> dict:
        """幂等归还资源: RUNNING→终止并释放, QUEUED→撤回 (本无占用),
        终态→幂等空操作。绝不产生二次扣减。"""
        with self._lock:
            task = self._require_task(task_id)
            if task.state == RUNNING:
                self._terminate_locked(task, "resources_released")
                acted = "terminated"
            elif task.state == QUEUED:
                self._cancel_locked(task, "released_while_queued")
                acted = "cancelled"
            else:
                acted = "already_released"
            self._schedule_locked()
            return {"idempotent": acted == "already_released",
                    "action": acted, **task.to_dict()}

    def complete_task(self, task_id: str, result: dict | None = None) -> dict:
        with self._lock:
            task = self._require_task(task_id)
            if task.state == COMPLETED:
                return {"idempotent": True, **task.to_dict()}
            if task.state != RUNNING:
                raise BatchConflict(
                    f"task in state {task.state} cannot complete",
                    reason="not_running")
            self._finish_locked(task, COMPLETED, EV_TASK_COMPLETED,
                                reason="completed", result=result)
            self._schedule_locked()
            return {"idempotent": False, **task.to_dict()}

    def fail_task(self, task_id: str, reason: str = "failed",
                  result: dict | None = None) -> dict:
        with self._lock:
            task = self._require_task(task_id)
            if task.state == FAILED:
                return {"idempotent": True, **task.to_dict()}
            if task.state != RUNNING:
                raise BatchConflict(
                    f"task in state {task.state} cannot fail",
                    reason="not_running")
            self._finish_locked(task, FAILED, EV_TASK_FAILED,
                                reason=reason, result=result)
            self._schedule_locked()
            return {"idempotent": False, **task.to_dict()}

    def _terminate_locked(self, task: Task, reason: str) -> None:
        self._finish_locked(task, TERMINATED, EV_TASK_TERMINATED,
                            reason=reason, result=None)

    def _finish_locked(self, task: Task, state: str, event: str, *,
                       reason: str, result: dict | None) -> None:
        """RUNNING → 终态: 先逐台归还机器账目 (幂等 pop), 再落事件。

        事件一旦落盘, 重放也会逐台 pop: 重复完成/终止/崩溃重启都不会
        二次归还。
        """
        ts = self._now()
        released = []
        for mid in sorted(task.allocations):
            machine = self._machines.get(mid)
            alloc = machine.release(task.task_id) if machine else None
            if alloc is not None:
                released.append(alloc.to_dict())
                if machine is not None:
                    machine.timeline.append({
                        "ts": ts, "kind": "released",
                        "total": machine.total.to_dict(),
                        "detail": {"task_id": task.task_id,
                                   "amount": alloc.to_dict()}})
        task.state = state
        task.finished_ts = ts
        task.finish_reason = reason
        if result is not None:
            task.result = dict(result)
        task.add_timeline({
            "ts": ts, "kind": state.lower(),
            "detail": {"reason": reason, "released": released,
                       "result": task.result}})
        self._emit(event, {
            "task_id": task.task_id, "tenant_id": task.tenant_id,
            "reason": reason, "ts": ts, "result": task.result,
            "released": released})

    def _require_task(self, task_id: str) -> Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise BatchNotFound(f"task {task_id} not found",
                                reason="unknown_task")
        return task

    # ========================================================== scheduler
    def run_scheduler_pass(self) -> dict:
        """对外显式触发一轮调度 (HTTP tick / 测试), 返回该轮可解释明细。"""
        with self._lock:
            return self._schedule_locked(force_record=True)

    def _schedule_locked(self, *, force_record: bool = False) -> dict:
        """一轮接纳: 过期取消 → 加权公平多趟扫描 → 成组放置。

        调用方持有管理器锁。

        一轮内 *多趟* 扫描公平次序: 某租户队首在第 k 趟被接纳后, 其
        fair_key 前进 1/weight, 紧接着的下一趟按新分数重排——于是权重 3
        的租户在同一轮可连续拿到约 3 个槽位, 权重比例在单轮内即成立
        (而不是每租户每轮最多一个, 那会把吞吐强行拉成 1:1)。
        """
        now = self._now()
        pass_id = _new_id("pass")
        order_log: list[dict] = []
        expired: list[str] = []
        started: list[str] = []
        defer_events = 0
        considered = 0
        # 本轮已记录过"推迟"的任务: 同一轮内不因后续趟次重复记录
        deferred_this_pass: set[str] = set()
        # 本轮某趟起没有发生任何接纳时即收敛, 停止扫描
        max_sweeps = max(1, len(self._machines) * 2)

        # 1) 过期任务取消 (有界等待)
        for task in list(self._tasks.values()):
            if (task.state == QUEUED and task.deadline_ts is not None
                    and now >= task.deadline_ts):
                self._cancel_locked(task, "deadline_expired")
                expired.append(task.task_id)

        def _heads() -> list[tuple[Tenant, Task]]:
            heads: dict[str, Task] = {}
            for task in self._tasks.values():
                if task.state != QUEUED:
                    continue
                cur = heads.get(task.tenant_id)
                if cur is None or task.enqueue_seq < cur.enqueue_seq:
                    heads[task.tenant_id] = task
            return [(self._tenants[tid], t) for tid, t in heads.items()
                    if tid in self._tenants]

        # 2) 多趟加权公平扫描
        for sweep in range(max_sweeps):
            ordered = order_queued(
                _heads(), now=now,
                aging_after=self.aging_after, aging_rate=self.aging_rate)
            if not ordered:
                break
            if sweep == 0:
                considered = len(ordered)
            placed_in_sweep = False
            for score, tenant, task in ordered:
                if task.state != QUEUED:
                    continue
                ids, defer_code = find_placement(
                    list(self._machines.values()), task)
                task.last_scheduled_pass = pass_id
                if defer_code is not None:
                    # 装不下只跳过本租户, 不挡后续租户; 每轮每任务记一次
                    if task.task_id not in deferred_this_pass:
                        deferred_this_pass.add(task.task_id)
                        detail = defer_detail(
                            defer_code, list(self._machines.values()), task)
                        task.last_defer = {
                            "code": defer_code, "detail": detail,
                            "pass_id": pass_id, "ts": now}
                        # 仅在原因码相对上次变化时落事件, 避免刷爆 journal
                        prev = next(
                            (e["detail"] for e in reversed(task.timeline)
                             if e["kind"] == "deferred"), None)
                        if prev is None or prev.get("reason") != defer_code:
                            task.add_timeline({
                                "ts": now, "kind": "deferred",
                                "detail": {"pass_id": pass_id, **detail}})
                            self._emit(EV_TASK_DEFERRED, {
                                "task_id": task.task_id,
                                "tenant_id": task.tenant_id,
                                "pass_id": pass_id,
                                "code": defer_code, "detail": detail,
                                "ts": now})
                            defer_events += 1
                        order_log.append({
                            "sweep": sweep,
                            "tenant_id": tenant.tenant_id,
                            "task_id": task.task_id,
                            "score": score, "decision": "deferred",
                            "reason": defer_code})
                    continue

                # 3) 全部机器当前都放得下 → 成组原子记账后启动
                allocations = []
                for mid in ids:
                    machine = self._machines[mid]
                    machine.allocate(task.task_id, task.need, now)
                    alloc = machine.allocations[task.task_id]
                    task.allocations[mid] = alloc
                    allocations.append(alloc.to_dict())
                    machine.timeline.append({
                        "ts": now, "kind": "allocated",
                        "total": machine.total.to_dict(),
                        "detail": {"task_id": task.task_id,
                                   "amount": alloc.to_dict()}})
                task.state = RUNNING
                task.started_ts = now
                task.last_defer = None
                task.add_timeline({
                    "ts": now, "kind": "allocated",
                    "detail": {"pass_id": pass_id, "sweep": sweep,
                               "machines": list(ids),
                               "need": task.need.to_dict()}})
                tenant.fair_key += 1.0 / tenant.weight
                tenant.admitted += 1
                self._emit(EV_TASK_ALLOCATED, {
                    "task_id": task.task_id, "tenant_id": task.tenant_id,
                    "pass_id": pass_id, "started_ts": now,
                    "allocations": allocations,
                    "tenant_fair_key": tenant.fair_key})
                started.append(task.task_id)
                order_log.append({
                    "sweep": sweep, "tenant_id": tenant.tenant_id,
                    "task_id": task.task_id, "score": score,
                    "decision": "started", "machines": list(ids)})
                placed_in_sweep = True
                # 本趟其余租户仍以"这台机器已被占用"的新视图继续尝试
            if not placed_in_sweep:
                break

        summary = {
            "pass_id": pass_id, "ts": now,
            "aging_after": self.aging_after,
            "aging_rate": self.aging_rate,
            "considered": considered, "started": started,
            "expired": expired, "order": order_log,
        }
        if force_record or started or expired or defer_events:
            self._passes.append(summary)
            del self._passes[:-RECENT_PASSES]
        return summary

    # ---------------------------------------------------------- ticker
    def start_ticker(self) -> None:
        """老化/过期只随时间流逝生效, 后台定时触发轮次 (守护线程)。"""
        if self._tick_timer is not None:
            return

        def _arm():
            self._tick_timer = threading.Timer(self._tick_interval, _tick)
            self._tick_timer.daemon = True
            self._tick_timer.start()

        def _tick():
            with self._lock:
                self._tick_timer = None
            try:
                self.run_scheduler_pass()
            finally:
                with self._lock:
                    if self._recording:
                        _arm()

        _arm()

    def stop_ticker(self) -> None:
        timer = self._tick_timer
        if timer is not None:
            timer.cancel()
            self._tick_timer = None

    # ============================================================= reads
    def get_task(self, task_id: str) -> dict:
        with self._lock:
            return self._require_task(task_id).to_dict()

    def list_tasks(self, *, tenant_id: str | None = None,
                   state: str | None = None) -> list[dict]:
        with self._lock:
            tasks = list(self._tasks.values())
        if tenant_id is not None:
            tasks = [t for t in tasks if t.tenant_id == tenant_id]
        if state is not None:
            tasks = [t for t in tasks if t.state == state]
        tasks.sort(key=lambda t: t.enqueue_seq)
        return [t.to_dict() for t in tasks]

    def list_tenants(self) -> list[dict]:
        with self._lock:
            tenants = list(self._tenants.values())
            tasks = list(self._tasks.values())
        out = []
        for t in sorted(tenants, key=lambda x: x.tenant_id):
            d = t.to_dict()
            d["queued"] = sum(
                1 for k in tasks
                if k.tenant_id == t.tenant_id and k.state == QUEUED)
            d["running"] = sum(
                1 for k in tasks
                if k.tenant_id == t.tenant_id and k.state == RUNNING)
            out.append(d)
        return out

    def list_machines(self) -> list[dict]:
        with self._lock:
            return [m.to_dict()
                    for m in sorted(self._machines.values(),
                                    key=lambda x: x.machine_id)]

    def list_passes(self) -> list[dict]:
        with self._lock:
            return list(self._passes)

    def dashboard_data(self) -> dict:
        with self._lock:
            now = self._now()
            tenants = []
            for t in sorted(self._tenants.values(),
                            key=lambda x: x.tenant_id):
                tasks = [k for k in self._tasks.values()
                         if k.tenant_id == t.tenant_id]
                oldest = min((k.enqueued_ts for k in tasks
                              if k.state == QUEUED), default=None)
                d = t.to_dict()
                d.update(
                    queued=sum(1 for k in tasks if k.state == QUEUED),
                    running=sum(1 for k in tasks if k.state == RUNNING),
                    oldest_wait=(now - oldest) if oldest is not None else None,
                    score=_tenant_public_score(
                        t, now=now, head_enqueued_ts=oldest,
                        aging_after=self.aging_after,
                        aging_rate=self.aging_rate)
                    if oldest is not None else None)
                tenants.append(d)
            totals = {"cpu": 0, "mem": 0, "gpu": 0,
                      "used_cpu": 0, "used_mem": 0, "used_gpu": 0}
            for m in self._machines.values():
                if m.state == REMOVED:
                    continue
                u = m.used()
                totals["cpu"] += m.total.cpu
                totals["mem"] += m.total.mem
                totals["gpu"] += m.total.gpu
                totals["used_cpu"] += u.cpu
                totals["used_mem"] += u.mem
                totals["used_gpu"] += u.gpu
            return {
                "now": now,
                "aging_after": self.aging_after,
                "aging_rate": self.aging_rate,
                "totals": totals,
                "tenants": tenants,
                "machines": [m.to_dict() for m in
                             sorted(self._machines.values(),
                                    key=lambda x: x.machine_id)],
                "queue": [t.to_dict() for t in sorted(
                    (t for t in self._tasks.values() if t.state == QUEUED),
                    key=lambda t: t.enqueue_seq)],
                "running": [t.to_dict() for t in sorted(
                    (t for t in self._tasks.values() if t.state == RUNNING),
                    key=lambda t: t.started_ts or 0)],
                "recent_terminal": [t.to_dict() for t in sorted(
                    (t for t in self._tasks.values()
                     if t.state in TERMINAL_STATES),
                    key=lambda t: t.finished_ts or 0,
                    reverse=True)[:50]],
                "passes": list(self._passes[-20:]),
            }

    def task_page_data(self, task_id: str) -> dict:
        with self._lock:
            task = self._require_task(task_id)
            data = task.to_dict()
            data["machine_views"] = []
            for mid, alloc in sorted(task.allocations.items()):
                machine = self._machines.get(mid)
                data["machine_views"].append({
                    "machine_id": mid,
                    "found": machine is not None,
                    "allocation": alloc.to_dict(),
                    "machine": machine.to_dict() if machine else None})
            return data

    # ============================================================ replay
    def _replay(self) -> None:
        for evt in self.journal.replay():
            self._apply(evt)
        # 对账: RUNNING 任务必须在每台机器上有账目 (事件齐全时恒成立)
        for task in self._tasks.values():
            if task.state != RUNNING:
                continue
            for mid, alloc in task.allocations.items():
                machine = self._machines.get(mid)
                if machine is not None and task.task_id \
                        not in machine.allocations:
                    machine.allocations[task.task_id] = alloc

    def _apply(self, evt: dict) -> None:
        etype = evt["type"]
        ts = float(evt.get("ts") or self._now())
        d = {k: v for k, v in evt.items() if k not in ("seq", "ts", "type")}

        if etype == EV_TENANT_REGISTERED:
            self._tenants[d["tenant_id"]] = Tenant(
                tenant_id=d["tenant_id"], weight=float(d["weight"]),
                created_ts=float(d.get("created_ts", ts)),
                fair_key=float(d.get("fair_key", 0.0)),
                baseline_aligned=False)
            return
        if etype == EV_TENANT_UPDATED:
            tenant = self._tenants.get(d["tenant_id"])
            if tenant is not None:
                tenant.weight = float(d["weight"])
            return

        if etype == EV_MACHINE_ADDED:
            machine = Machine(
                machine_id=d["machine_id"],
                total=Resource.from_dict(d["total"]),
                labels=dict(d.get("labels") or {}), created_ts=ts)
            machine.timeline.append({
                "ts": ts, "kind": "added",
                "total": machine.total.to_dict(),
                "detail": {"labels": dict(machine.labels)}})
            self._machines[machine.machine_id] = machine
            return
        if etype == EV_MACHINE_UPDATED:
            machine = self._machines.get(d["machine_id"])
            if machine is None:
                return
            new_total = Resource.from_dict(d["total"])
            if machine.total != new_total:
                machine.timeline.append({
                    "ts": ts, "kind": "updated",
                    "total": new_total.to_dict(),
                    "detail": {"old_total":
                               Resource.from_dict(d["old_total"]).to_dict()
                               if d.get("old_total") else None}})
                machine.total = new_total
            if "labels" in d:
                machine.labels = dict(d["labels"])
                machine.timeline.append({
                    "ts": ts, "kind": "labels_updated",
                    "total": machine.total.to_dict(),
                    "detail": {"labels": dict(machine.labels)}})
            return
        if etype == EV_MACHINE_STATE:
            machine = self._machines.get(d["machine_id"])
            if machine is not None and machine.state != d["state"]:
                machine.state = d["state"]
                machine.timeline.append({
                    "ts": ts, "kind": "state_changed",
                    "total": machine.total.to_dict(),
                    "detail": {"state": d["state"]}})
            return

        if etype == EV_TASK_SUBMITTED:
            need = Resource.from_dict(d["need"])
            task = Task(
                task_id=d["task_id"], tenant_id=d["tenant_id"],
                business_key=d["business_key"],
                content_digest=d["content_digest"],
                machine_count=int(d["machine_count"]), need=need,
                selector=dict(d.get("selector") or {}),
                enqueued_ts=float(d["enqueued_ts"]),
                deadline_ts=d.get("deadline_ts"),
                enqueue_seq=int(d.get("enqueue_seq", 0)))
            task.add_timeline({"ts": ts, "kind": "submitted",
                               "detail": {"content_digest":
                                          task.content_digest}})
            self._tasks[task.task_id] = task
            self._idempotency[(task.tenant_id,
                               task.business_key)] = task.task_id
            self._enqueue_counter = max(self._enqueue_counter,
                                        task.enqueue_seq)
            tenant = self._tenants.get(task.tenant_id)
            if tenant is not None:
                # 首次提交时基线对齐到当时最大 fair_key; 重放按相同规则
                # 从注册后的事件序列推导 (后续 allocated 事件会给出权威键)。
                tenant.baseline_aligned = True
            return
        if etype == EV_TASK_DEFERRED:
            task = self._tasks.get(d["task_id"])
            if task is None:
                return
            task.last_scheduled_pass = d.get("pass_id")
            task.last_defer = {"code": d["code"], "detail": d.get("detail"),
                               "pass_id": d.get("pass_id"), "ts": ts}
            task.add_timeline({"ts": ts, "kind": "deferred",
                               "detail": {"pass_id": d.get("pass_id"),
                                          **(d.get("detail") or {})}})
            return
        if etype == EV_TASK_ALLOCATED:
            task = self._tasks.get(d["task_id"])
            if task is None:
                return
            allocs = [Allocation.from_dict(a) for a in d.get("allocations")]
            task.allocations = {a.machine_id: a for a in allocs}
            task.state = RUNNING
            task.started_ts = float(d.get("started_ts", ts))
            task.last_defer = None
            task.add_timeline({
                "ts": task.started_ts, "kind": "allocated",
                "detail": {"pass_id": d.get("pass_id"),
                           "machines": [a.machine_id for a in allocs],
                           "need": task.need.to_dict()}})
            for alloc in allocs:
                machine = self._machines.get(alloc.machine_id)
                if machine is None:
                    continue
                machine.allocations.setdefault(task.task_id, alloc)
                machine.timeline.append({
                    "ts": task.started_ts, "kind": "allocated",
                    "total": machine.total.to_dict(),
                    "detail": {"task_id": task.task_id,
                               "amount": alloc.to_dict()}})
            tenant = self._tenants.get(task.tenant_id)
            if tenant is not None:
                if "tenant_fair_key" in d:
                    tenant.fair_key = float(d["tenant_fair_key"])
                else:  # 兼容旧日志
                    tenant.fair_key += 1.0 / tenant.weight
                tenant.admitted += 1
            return

        # 终态事件: 逐台幂等归还机器账目
        if etype in (EV_TASK_COMPLETED, EV_TASK_FAILED,
                     EV_TASK_CANCELLED, EV_TASK_TERMINATED):
            task = self._tasks.get(d["task_id"])
            if task is None:
                return
            state = {EV_TASK_COMPLETED: COMPLETED, EV_TASK_FAILED: FAILED,
                     EV_TASK_CANCELLED: CANCELLED,
                     EV_TASK_TERMINATED: TERMINATED}[etype]
            reason = d.get("reason", {
                EV_TASK_COMPLETED: "completed",
                EV_TASK_FAILED: "failed",
                EV_TASK_CANCELLED: "cancelled",
                EV_TASK_TERMINATED: "terminated"}[etype])
            released = []
            for mid in sorted(task.allocations):
                machine = self._machines.get(mid)
                alloc = machine.release(task.task_id) if machine else None
                if alloc is not None:
                    released.append(alloc.to_dict())
                    if machine is not None:
                        machine.timeline.append({
                            "ts": ts, "kind": "released",
                            "total": machine.total.to_dict(),
                            "detail": {"task_id": task.task_id,
                                       "amount": alloc.to_dict()}})
            task.state = state
            task.finished_ts = ts
            task.finish_reason = reason
            if d.get("result") is not None:
                task.result = d.get("result")
            task.add_timeline({
                "ts": ts, "kind": state.lower(),
                "detail": {"reason": reason, "released": released,
                           "result": task.result}})
            return


def _tenant_public_score(tenant: Tenant, *, now: float,
                         head_enqueued_ts: float | None,
                         aging_after: float, aging_rate: float):
    if head_enqueued_ts is None:
        return tenant.fair_key
    from .scheduling import tenant_score
    return tenant_score(tenant, head_enqueued_ts, now=now,
                        aging_after=aging_after, aging_rate=aging_rate)


class _StubTask:
    """接纳闸门只需要 machine_count/need/selector 的轻量视图。"""

    def __init__(self, machine_count: int, need: Resource,
                 selector: dict[str, str]):
        self.machine_count = machine_count
        self.need = need
        self.selector = selector
