"""调度核心: 加权公平排序 (虚拟时间 + 老化) 与异构成组放置。

纯函数, 不持有状态; 所有状态由 ``BatchManager`` 传入并在其锁内调用。

公平模型 (可解释):
  每个租户维护一个加权公平虚拟时间 ``fair_key``, 初始注册时取当时活跃
  租户的最大键值; 每 *接纳* 一个任务, ``fair_key += 1 / weight``。
  调度按队首任务的 ``(fair_key - 老化加速, 入队序号)`` 升序选取租户。
  于是:
    * 权重为 2 的租户长期吞吐约为权重 1 的两倍 (可解释的权重机会);
    * 等候时长超过阈值后, 得分随等待时间线性下移, 久候任务必定在有限
      时间内排到最前 —— 低权重租户不会无限期饥饿;
    * 一轮中某租户队首装不下时仅跳过该租户, 继续尝试后续租户,
      因此大任务不会阻挡其他租户此刻可启动的任务 (无队头阻塞跨租户)。

放置模型 (all-or-nothing):
  只从 *当前空闲容量足够* 的 ENABLED 机器中挑选, 按最紧凑 (best-fit)
  确定性打分选出恰好 ``machine_count`` 台; 数量不足时返回稳定原因码,
  不做任何预留/部分占用。
"""

from __future__ import annotations

from .model import ENABLED, REMOVED, Machine, Resource, Task, Tenant

# 推迟 (本次不启动) 稳定原因码
DEFER_NO_CANDIDATE = "no_enabled_machine"
DEFER_NOT_MATCHING = "insufficient_matching_machines"
DEFER_FREE_CAPACITY = "insufficient_free_capacity"
DEFER_DEADLINE = None  # 占位: 过期走取消而非推迟

# 接纳期 (提交时全集群都不可能满足) 原因码
REJECT_TOO_FEW_MATCHING = "insufficient_cluster_machines"
REJECT_CLUSTER_CPU = "cluster_cpu_exceeded"
REJECT_CLUSTER_MEM = "cluster_mem_exceeded"
REJECT_CLUSTER_GPU = "cluster_gpu_exceeded"


# --------------------------------------------------------------- fairness
def tenant_score(tenant: Tenant, head_enqueued_ts: float, *, now: float,
                 aging_after: float, aging_rate: float) -> float:
    """租户当前调度得分 (越小越优先)。

    ``tenant.fair_key`` 给出权重公平位置 (每接纳一个任务前进 1/weight);
    老化只依据 *该租户队首自身* 的等候时长: 超过 ``aging_after`` 后得分
    按 ``aging_rate``/秒 下移。这样:

      * 所有租户都在同样积压时, 大家的队首都同样老, 老化量相互抵消,
        权重比例严格保留 (高权重租户长期得到 weight 倍吞吐);
      * 某租户因权重低持续被跳过、其队首越积越老时, 只有它自己的得分
        不断下移, 经过有限墙上时间必定排到最前 —— 饥饿有上界。
    """
    waited = max(0.0, now - head_enqueued_ts)
    over = max(0.0, waited - aging_after)
    return tenant.fair_key - over * aging_rate


def order_queued(
    tenants_heads: list[tuple[Tenant, Task]],
    *,
    now: float,
    aging_after: float,
    aging_rate: float,
) -> list[tuple[float, Tenant, Task]]:
    """按 (老化后得分, 租户 id) 升序返回 (score, tenant, task)。

    决胜用租户 id 而非入队序号: 入队序号会持续偏向队列积压更深的租户,
    使其低权重也能长期多拿 (与权重相悖); 确定性 id 决胜在每个权重
    最小公倍数周期内严格收敛到权重比例, 而某租户队首久候产生的老化
    得分 (有墙上时间上界) 负责兜底防饥饿。
    """
    if not tenants_heads:
        return []
    ordered = []
    for tenant, task in tenants_heads:
        score = tenant_score(
            tenant, task.enqueued_ts, now=now,
            aging_after=aging_after, aging_rate=aging_rate)
        ordered.append((score, tenant, task))
    ordered.sort(key=lambda x: (x[0], x[1].tenant_id))
    return ordered


# -------------------------------------------------------------- placement
def _labels_match(labels: dict[str, str], selector: dict[str, str]) -> bool:
    return all(labels.get(k) == v for k, v in selector.items())


def eligible_machines(machines, task: Task) -> list[Machine]:
    """可参与该任务放置的机器: ENABLED 且标签全部匹配 (当前可分配)。"""
    return [m for m in machines
            if m.state == ENABLED and _labels_match(m.labels, task.selector)]


def feasible_machines(machines, task: Task) -> list[Machine]:
    """物理上可能满足任务的机器: 未 REMOVED (含 DRAINING) 且标签匹配。

    DRAINING 机器此刻不分配新任务, 但可能稍后恢复 ENABLED, 因此不计入
    "永久不可行", 任务正常入队等待。
    """
    return [m for m in machines
            if m.state != REMOVED and _labels_match(m.labels, task.selector)]


def cluster_feasible(machines, task: Task) -> str | None:
    """接纳期检查: 非 REMOVED 匹配机器总容量是否在物理上可能满足该任务。

    返回 None 表示可行; 否则返回稳定拒绝原因码。注意这只做总量闸门,
    瞬时碎片 / DRAINING (可能恢复) 不拒绝, 任务正常排队。
    """
    eligible = feasible_machines(machines, task)
    if len(eligible) < task.machine_count:
        return REJECT_TOO_FEW_MATCHING
    totals = {"cpu": 0, "mem": 0, "gpu": 0}
    for m in eligible:
        totals["cpu"] += m.total.cpu
        totals["mem"] += m.total.mem
        totals["gpu"] += m.total.gpu
    need_cpu = task.need.cpu * task.machine_count
    need_mem = task.need.mem * task.machine_count
    need_gpu = task.need.gpu * task.machine_count
    if need_cpu > totals["cpu"]:
        return REJECT_CLUSTER_CPU
    if need_mem > totals["mem"]:
        return REJECT_CLUSTER_MEM
    if need_gpu > totals["gpu"]:
        return REJECT_CLUSTER_GPU
    return None


def _best_fit_key(machine: Machine, need: Resource):
    """放置评分 (越低越优先)。

    两个目标, 按字典序组合:
      1. **负载均衡 (spread)**: 优先当前占用比例最低的机器, 避免把多个
         独立任务堆到同一台大机器、让其他机器空转, 从而为后续的 *成组*
         任务保留分散在多台机器上的可协同容量;
      2. **紧凑 (bin-pack)**: 占用比例相同时, 优先放置后剩余比例更小的
         机器, 减少碎片。
    最后以 machine_id 确定性决胜。
    """
    used = machine.used()
    load = 0.0
    slack = 0.0
    for uv, fv, nv, tv in (
            (used.cpu, None, need.cpu, machine.total.cpu),
            (used.mem, None, need.mem, machine.total.mem),
            (used.gpu, None, need.gpu, machine.total.gpu)):
        if tv > 0:
            load += uv / tv
    free = machine.free()
    for fv, nv, tv in ((free.cpu, need.cpu, machine.total.cpu),
                      (free.mem, need.mem, machine.total.mem),
                      (free.gpu, need.gpu, machine.total.gpu)):
        if tv > 0:
            slack += (fv - nv) / tv
    return load, slack, machine.machine_id


def find_placement(machines, task: Task):
    """返回 ``(machine_ids, reason)``。

    成功: ``([id, ...], None)``, 列表长度恰为 ``machine_count``。
    失败: ``([], reason)``, reason 为稳定推迟码; 调用方不得据此记账。
    """
    eligible = eligible_machines(machines, task)
    if not eligible:
        return [], DEFER_NO_CANDIDATE
    if len(eligible) < task.machine_count:
        return [], DEFER_NOT_MATCHING
    fitting = [m for m in eligible if m.can_fit(task.need)]
    if len(fitting) < task.machine_count:
        return [], DEFER_FREE_CAPACITY
    fitting.sort(key=lambda m: _best_fit_key(m, task.need))
    chosen = fitting[:task.machine_count]
    return [m.machine_id for m in chosen], None


def defer_detail(reason: str, machines, task: Task) -> dict:
    """生成可展示的推迟原因细目 (数量/缺口), 不含内部地址。"""
    eligible = eligible_machines(machines, task)
    fitting = [m for m in eligible if m.can_fit(task.need)]
    detail = {
        "reason": reason,
        "required_machines": task.machine_count,
        "matching_enabled_machines": len(eligible),
        "currently_fitting_machines": len(fitting),
        "need_per_machine": task.need.to_dict(),
        "selector": dict(task.selector),
    }
    if reason == DEFER_FREE_CAPACITY:
        # 汇总缺口, 帮助解释"为什么此刻装不下"
        shortage = {"cpu": 0, "mem": 0, "gpu": 0}
        for m in eligible:
            free = m.free()
            if not m.can_fit(task.need):
                shortage["cpu"] += max(0, task.need.cpu - free.cpu)
                shortage["mem"] += max(0, task.need.mem - free.mem)
                shortage["gpu"] += max(0, task.need.gpu - free.gpu)
        detail["aggregate_shortage_on_matching"] = shortage
    return detail
