"""接纳器浏览器页面 (服务端渲染 HTML, 2 秒自动刷新)。

总览页展示: 租户权重与公平性计分、异构机器实时余量与占用明细、按
score 排好的等候队列 (含暂缓缘由)、运行中任务与其成组落点。
详情页展示: 单任务的分配去向 (副本→机器)、容量变化时间线、拒绝缘由。
"""

from __future__ import annotations

import html
import time


def _h(v) -> str:
    return html.escape(str(v), quote=True)


_STATE_COLORS = {
    "QUEUED": "#b26a00",
    "ALLOCATED": "#1a4fb3",
    "RUNNING": "#0a7d2c",
    "SUCCEEDED": "#0a7d2c",
    "TERMINATED": "#b00020",
    "CANCELLED": "#666",
    "REJECTED": "#b00020",
    "ONLINE": "#0a7d2c",
    "DRAINED": "#b26a00",
    "OFFLINE": "#666",
}

_REASON_TEXT = {
    "single_machine_too_small": "单副本需求超出集群内任何一台机器的物理容量",
    "cluster_insufficient_capacity": "即使全部机器空闲, 副本总数仍超过集群上限",
    "no_machine_matching_replica": "当前没有能满足该副本 (含加速器型号) 的机器",
    "insufficient_free_capacity_cluster_busy": "机器可容纳但此刻空闲容量不足 (集群忙)",
    "no_online_machines": "没有 ONLINE 机器 (全部已下线/排空)",
}


def _badge(state: str) -> str:
    color = _STATE_COLORS.get(state, "#444")
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:10px;font-size:12px;white-space:nowrap">'
            f'{_h(state)}</span>')


def _ts(ts) -> str:
    if not ts:
        return "-"
    return time.strftime("%H:%M:%S", time.localtime(float(ts)))


def _reason(reason: str | None) -> str:
    if not reason:
        return ""
    return f"{_h(reason)} — {_h(_REASON_TEXT.get(reason, ''))}"


def _acc_str(acc: dict) -> str:
    if not acc:
        return "-"
    return ", ".join(f"{_h(k)}×{v}" for k, v in sorted(acc.items()))


def _acc_free_str(m: dict) -> str:
    bits = []
    for k in sorted(m["accelerators_total"]):
        total = m["accelerators_total"][k]
        free = m["accelerators_free"].get(k, 0)
        bits.append(f"{_h(k)} {free}/{total}")
    return ", ".join(bits) if bits else "-"


def render_overview(data: dict) -> str:
    tenants_rows = "".join(
        f"<tr><td>{_h(t['tenant_id'])}</td><td>{t['weight']:g}</td>"
        f"<td>{t['queued']}</td><td>{t['active']}</td>"
        f"<td>{t['admitted']} / {t['completed']}</td>"
        f"<td>{t['oldest_wait_seconds']:.1f}s</td>"
        f"<td>{t['head_score']:.1f}</td></tr>"
        for t in data["tenants"]
    ) or '<tr><td colspan="7">暂无租户 (提交任务时自动登记, 默认权重 1)</td></tr>'

    machines_rows = "".join(_machine_row(m) for m in data["machines"]) \
        or '<tr><td colspan="9">暂无机器, 请先 <code>POST /api/admission/machines</code> 登记</td></tr>'

    queue_rows = "".join(
        f"<tr><td>{q['wait_seconds']:.1f}s</td>"
        f"<td>{q['score']:.1f}</td>"
        f"<td>{_h(q['tenant_id'])}</td>"
        f"<td><a href='/ui/admission/tasks/{_h(q['task_id'])}'>"
        f"<code>{_h(q['task_id'])}</code></a></td>"
        f"<td><code>{_h(q['business_key'])}</code></td>"
        f"<td>{q['replicas']}</td>"
        f"<td>{_h(_reason((q.get('last_defer') or {}).get('reason'))) or '-'}</td></tr>"
        for q in data["queue_order"]
    ) or '<tr><td colspan="7">队列为空</td></tr>'

    active_rows = "".join(_active_row(t) for t in data["active_tasks"]) \
        or '<tr><td colspan="6">没有已分配/运行中的任务</td></tr>'

    recent_rows = "".join(_recent_row(t) for t in data["recent_tasks"][:25])

    sched = data.get("last_schedule") or {}
    admitted_summary = ", ".join(
        f"{_h(a['tenant_id'])}:{_h(a['task_id'][:14])}×{a['replicas']}"
        for a in sched.get("admitted", [])) or "-"
    deferred_summary = ", ".join(
        f"{_h(d['tenant_id'])}:{_h(d['reason'])}"
        for d in sched.get("deferred", [])) or "-"

    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2">
<title>多租户成组任务接纳器</title>
<style>
 body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;color:#222}}
 table{{border-collapse:collapse;margin:8px 0 24px}}
 th,td{{border:1px solid #ccc;padding:5px 9px;font-size:13px}}
 th{{background:#f3f3f8;text-align:left}}
 h1{{font-size:22px}} h2{{font-size:17px;margin-top:26px}}
 code{{font-size:12px}} small{{color:#666}}
</style></head><body>
<h1>多租户成组批任务接纳器</h1>
<p><small>
<a href="/api/admission/overview">/api/admission/overview (JSON)</a> ·
<a href="/api/admission/tasks">任务</a> ·
<a href="/api/admission/machines">机器</a> ·
<a href="/api/admission/tenants">租户</a> ·
journal seq {data['journal_seq']} ·
{'持久化' if data['persistent'] else '纯内存'} ·
{_ts(data['server_ts'])}</small></p>

<h2>租户权重与公平性 <small>score = 等待秒数 × 权重 (老化保证不饥饿)</small></h2>
<table><tr><th>租户</th><th>权重</th><th>排队</th><th>占用中</th>
<th>已接纳/已完成</th><th>最久等待</th><th>队首 score</th></tr>
{tenants_rows}</table>

<h2>异构机器容量</h2>
<table><tr><th>机器</th><th>状态</th><th>CPU 空闲/总</th>
<th>内存 MB 空闲/总</th><th>加速器 空闲/总</th><th>占用切片</th>
<th>占用明细 (任务/副本/CPU/内存/加速器)</th></tr>
{machines_rows}</table>

<h2>等候队列 (按 score 降序; 同序 FIFO)</h2>
<table><tr><th>已等待</th><th>score</th><th>租户</th><th>任务</th>
<th>业务键</th><th>副本</th><th>暂缓缘由</th></tr>
{queue_rows}</table>

<h2>已分配 / 运行中 (成组落点)</h2>
<table><tr><th>任务</th><th>租户</th><th>状态</th><th>副本</th>
<th>分配机器</th><th>启动时间</th></tr>{active_rows}</table>

<h2>最近一轮调度</h2>
<p><small>接纳: {admitted_summary}<br>暂缓: {deferred_summary}<br>
剩余排队: {sched.get('queued_remaining', '-')} · 时刻 {_ts(sched.get('ts'))}</small></p>

<h2>最近任务</h2>
<table><tr><th>任务</th><th>租户</th><th>业务键</th><th>状态</th>
<th>机器</th><th>拒绝/暂缓缘由</th></tr>{recent_rows}</table>
</body></html>"""


def _machine_row(m: dict) -> str:
    alloc = "<br>".join(
        f"<code>{_h(a['slice_id'])}</code>: "
        f"{_h(a['cpus'])}c/{_h(a['memory_mb'])}MB/"
        f"{_acc_str(a['accelerators'])}"
        for a in m["allocations"]) or "-"
    return (f"<tr><td><code>{_h(m['machine_id'])}</code></td>"
            f"<td>{_badge(m['state'])}</td>"
            f"<td>{m['free_cpus']}/{m['cpus']}</td>"
            f"<td>{m['free_memory_mb']}/{m['memory_mb']}</td>"
            f"<td>{_acc_free_str(m)}</td>"
            f"<td>{m['active_slices']}</td><td><small>{alloc}</small></td></tr>")


def _active_row(t: dict) -> str:
    machines = ", ".join(
        f'<a href="/ui/admission/machines/{_h(mid)}"><code>{_h(mid)}</code></a>'
        for mid in t["machines"]) or "-"
    return (f"<tr><td><a href='/ui/admission/tasks/{_h(t['task_id'])}'>"
            f"<code>{_h(t['task_id'])}</code></a></td>"
            f"<td>{_h(t['tenant_id'])}</td><td>{_badge(t['state'])}</td>"
            f"<td>{t['replicas']}</td><td>{machines}</td>"
            f"<td>{_ts(t['started_ts'])}</td></tr>")


def _recent_row(t: dict) -> str:
    reason = ""
    if t.get("reject_reason"):
        reason = _reason(t["reject_reason"])
    elif t.get("last_defer"):
        reason = _reason(t["last_defer"].get("reason"))
    return (f"<tr><td><a href='/ui/admission/tasks/{_h(t['task_id'])}'>"
            f"<code>{_h(t['task_id'])[:18]}</code></a></td>"
            f"<td>{_h(t['tenant_id'])}</td>"
            f"<td><code>{_h(t['business_key'])}</code></td>"
            f"<td>{_badge(t['state'])}</td>"
            f"<td><small>{_h(', '.join(t['machines'])) or '-'}</small></td>"
            f"<td><small>{reason or '-'}</small></td></tr>")


def render_task_detail(t: dict, machine_index: dict[str, dict]) -> str:
    req = (f"{t['replicas']} 副本 × {t['cpus_per_replica']} CPU / "
           f"{t['memory_mb_per_replica']} MB / "
           f"{_acc_str(t['accelerators_per_replica'])}"
           f"{' (独占)' if t['exclusive'] else ''}")

    slice_rows = "".join(
        _slice_row(s, machine_index.get(s["machine_id"]))
        for s in t["slices"]) or '<tr><td colspan="6">尚未取得容量</td></tr>'

    timeline_rows = "".join(
        f"<tr><td>{_ts(e['ts'])}</td><td>{_h(e['kind'])}</td>"
        f"<td><small>{_h(_format_detail(e['kind'], e.get('detail') or {}))}"
        f"</small></td></tr>"
        for e in reversed(t["timeline"])
    )

    reject = ""
    if t["reject_reason"]:
        d = t.get("reject_detail") or {}
        reject = (f"<h2>拒绝缘由</h2><p><strong>{_reason(t['reject_reason'])}"
                  f"</strong></p><pre>{_h(_pretty(d))}</pre>")
    elif t.get("last_defer"):
        d = t["last_defer"]
        reject = (f"<h2>当前暂缓缘由 (任务仍在排队, 未占用任何容量)</h2>"
                  f"<p><strong>{_reason(d.get('reason'))}</strong> "
                  f"(第 {t['defer_count']} 次暂缓)</p>"
                  f"<pre>{_h(_pretty(d.get('shortage') or {}))}</pre>")

    result = ""
    if t.get("result") is not None:
        result = f"<h2>结果</h2><pre>{_h(_pretty(t['result']))}</pre>"

    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2"><title>任务 {_h(t['task_id'])}</title>
<style>
 body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:20px;color:#222}}
 table{{border-collapse:collapse;margin:8px 0 24px}}
 th,td{{border:1px solid #ccc;padding:5px 9px;font-size:13px}}
 th{{background:#f3f3f8;text-align:left}}
 pre{{background:#f7f7fb;padding:10px;border:1px solid #ddd;
      font-size:12px;overflow:auto;white-space:pre-wrap}}
 small{{color:#555}}
</style></head><body>
<p><small><a href="/ui/admission">← 返回接纳器总览</a></small></p>
<h1>任务 <code>{_h(t['task_id'])}</code> {_badge(t['state'])}</h1>
<table>
<tr><th>租户</th><td>{_h(t['tenant_id'])}</td></tr>
<tr><th>业务键</th><td><code>{_h(t['business_key'])}</code></td></tr>
<tr><th>需求</th><td>{req}</td></tr>
<tr><th>内容摘要</th><td><code>{_h(t['content_digest'][:24])}…</code></td></tr>
<tr><th>入队/分配/启动/完成</th><td>{_ts(t['enqueued_ts'])} ·
{_ts(t['allocated_ts'])} · {_ts(t['started_ts'])} ·
{_ts(t['finished_ts'])}</td></tr>
<tr><th>分配 seq</th><td>{t['allocated_seq'] if t['allocated_seq'] is not None else '-'}</td></tr>
</table>

<h2>分配去向 (副本 → 机器) 与容量</h2>
<table><tr><th>切片</th><th>副本</th><th>机器</th><th>CPU</th>
<th>内存 MB</th><th>加速器</th></tr>{slice_rows}</table>
{reject}
{result}
<h2>容量变化 / 状态时间线</h2>
<table><tr><th>时间</th><th>事件</th><th>明细</th></tr>{timeline_rows}</table>
</body></html>"""


def _slice_row(s: dict, machine_snap: dict | None) -> str:
    if machine_snap is None:
        free = ""
    else:
        free = (f" (该机器现空闲 {machine_snap['free_cpus']}c/"
                f"{machine_snap['free_memory_mb']}MB)")
    return (f"<tr><td><code>{_h(s['slice_id'])}</code></td>"
            f"<td>{s['replica']}</td>"
            f"<td><code>{_h(s['machine_id'])}</code><small>{free}</small></td>"
            f"<td>{s['cpus']}</td><td>{s['memory_mb']}</td>"
            f"<td>{_acc_str(s['accelerators'])}</td></tr>")


def _format_detail(kind: str, detail: dict) -> str:
    if kind == "allocated":
        return "落点: " + ", ".join(
            f"r{p['replica']}→{p['machine_id']}"
            for p in detail.get("placement", []))
    if kind in ("terminated", "completed"):
        return (f"{detail.get('reason', detail.get('status', ''))} "
                f"归还切片 {detail.get('released_slices', '')}")
    if kind == "deferred":
        return detail.get("reason", "")
    return _pretty(detail) if detail else ""


def _pretty(obj: dict) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)


def render_machine_detail(m: dict) -> str:
    alloc_rows = "".join(
        f"<tr><td><code>{_h(a['slice_id'])}</code></td>"
        f"<td><a href='/ui/admission/tasks/{_h(a['task_id'])}'>"
        f"<code>{_h(a['task_id'][:18])}</code></a></td>"
        f"<td>{_h(a['tenant_id'])}</td><td>{a['replica']}</td>"
        f"<td>{a['cpus']}</td><td>{a['memory_mb']}</td>"
        f"<td>{_acc_str(a['accelerators'])}</td></tr>"
        for a in m["allocations"]) or '<tr><td colspan="7">无占用</td></tr>'
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2"><title>机器 {_h(m['machine_id'])}</title>
<style>body{{font-family:sans-serif;margin:20px}}
table{{border-collapse:collapse}}th,td{{border:1px solid #ccc;padding:5px 9px;
font-size:13px}}th{{background:#f3f3f8}}</style></head><body>
<p><small><a href="/ui/admission">← 返回总览</a></small></p>
<h1>机器 <code>{_h(m['machine_id'])}</code> {_badge(m['state'])}</h1>
<p>CPU {m['free_cpus']}/{m['cpus']} · 内存 {m['free_memory_mb']}/{m['memory_mb']}
MB · 加速器 {_acc_free_str(m)}</p>
<h2>占用明细</h2>
<table><tr><th>切片</th><th>任务</th><th>租户</th><th>副本</th>
<th>CPU</th><th>内存</th><th>加速器</th></tr>{alloc_rows}</table>
</body></html>"""
