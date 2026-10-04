"""批接纳器页面 (服务端渲染 HTML, 2s 自动刷新)。

页面:
  * /ui/batch               总览: 租户权重与老化得分、机器容量、队列/运行/终态、调度轮次
  * /ui/batch/tasks/{id}    单任务: 分配去向、容量变化时间线、拒绝缘由
另含小型表单可直接登记租户/机器、提交任务以及撤回/终止。
"""

from __future__ import annotations

import html
import time

_STATE_COLORS = {
    "QUEUED": "#b26a00",
    "RUNNING": "#0a7d2c",
    "COMPLETED": "#1a4fb3",
    "FAILED": "#b00020",
    "CANCELLED": "#666",
    "TERMINATED": "#8a4b00",
    "ENABLED": "#0a7d2c",
    "DRAINING": "#b26a00",
    "REMOVED": "#666",
    "started": "#0a7d2c",
    "deferred": "#b26a00",
}

_REASON_TEXT = {
    "no_enabled_machine": "没有匹配标签的启用机器",
    "insufficient_matching_machines": "匹配机器数量不足 (需要 N 台协同)",
    "insufficient_free_capacity": "匹配机器当前空闲容量不足 (成组需求未满足)",
    "insufficient_cluster_machines": "全集群匹配机器总数不足, 已拒绝入队",
    "cluster_cpu_exceeded": "全集群 CPU 总量不足, 已拒绝入队",
    "cluster_mem_exceeded": "全集群内存总量不足, 已拒绝入队",
    "cluster_gpu_exceeded": "全集群加速器总量不足, 已拒绝入队",
    "business_key_conflict": "业务键重复但任务内容已变化",
    "deadline_expired": "等候超过截止时间被取消",
}


def _h(v) -> str:
    return html.escape("" if v is None else str(v), quote=True)


def _badge(state: str) -> str:
    color = _STATE_COLORS.get(state, "#444")
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:10px;font-size:12px;white-space:nowrap">'
            f'{_h(state)}</span>')


def _ts(ts) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))


def _res(r: dict) -> str:
    return f'{r.get("cpu", 0)}C / {r.get("mem", 0)}M / {r.get("gpu", 0)}G'


def _reason(code: str | None) -> str:
    if not code:
        return ""
    return f"{_h(code)}（{_h(_REASON_TEXT.get(code, code))}）"


def _selector(sel: dict) -> str:
    if not sel:
        return "<small>-</small>"
    return ", ".join(f"{_h(k)}={_h(v)}" for k, v in sorted(sel.items()))


_CSS = """
body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#222}
h1{font-size:22px}h2{font-size:17px;margin-top:28px;border-bottom:2px solid #eee;padding-bottom:4px}
table{border-collapse:collapse;font-size:13px;margin:8px 0}
th,td{border:1px solid #ccc;padding:5px 8px;text-align:left;vertical-align:top}
th{background:#f5f5f5}
code{background:#f3f3f3;padding:1px 4px;border-radius:3px}
.small{font-size:12px;color:#666}
form{display:inline-block;margin:2px 8px 2px 0;font-size:13px}
input,select{font-size:12px;padding:2px 4px}
button{font-size:12px;padding:2px 10px;cursor:pointer}
.bar{background:#eee;border-radius:6px;height:14px;width:160px;display:inline-block;vertical-align:middle}
.bar>i{display:block;height:14px;border-radius:6px;background:#0a7d2c}
pre{background:#f7f7f7;padding:8px;border-radius:6px;font-size:12px;overflow-x:auto}
"""


def render_batch_dashboard(data: dict) -> str:
    totals = data["totals"]

    def _bar(dim: str) -> str:
        cap, used = totals[dim], totals["used_" + dim]
        pct = (100.0 * used / cap) if cap else 0.0
        return (f'<span class="bar"><i style="width:{pct:.0f}%"></i></span> '
                f'{used}/{cap}')

    tenant_rows = "".join(
        f'<tr><td><code>{_h(t["tenant_id"])}</code></td>'
        f'<td>{t["weight"]:g}</td>'
        f'<td>{t["admitted"]}</td><td>{t["queued"]}</td><td>{t["running"]}</td>'
        f'<td>{("%.1fs" % t["oldest_wait"]) if t["oldest_wait"] is not None else "-"}</td>'
        f'<td>{("%.3f" % t["score"]) if t["score"] is not None else "-"}'
        f'<div class="small">键 {t["fair_key"]:.3f}</div></td></tr>'
        for t in data["tenants"]
    ) or '<tr><td colspan="7">暂无租户</td></tr>'

    machine_rows = "".join(
        f'<tr><td><code>{_h(m["machine_id"])}</code></td>'
        f'<td>{_badge(m["state"])}</td>'
        f'<td>{_res(m["total"])}</td><td>{_res(m["used"])}</td>'
        f'<td>{_res(m["free"])}</td>'
        f'<td>{_selector(m["labels"])}</td>'
        f'<td>{len(m["allocations"])}</td></tr>'
        for m in data["machines"]
    ) or '<tr><td colspan="7">暂无机器</td></tr>'

    def _task_row(t: dict, *, show_wait: bool) -> str:
        wait = (f'{t["wait_seconds"]:.1f}s' if t.get("wait_seconds")
                is not None else "-")
        reason = ""
        if t.get("last_defer"):
            d = t["last_defer"]
            reason = _reason(d.get("code"))
            det = d.get("detail") or {}
            if det.get("aggregate_shortage_on_matching"):
                reason += "<br><small>缺口: " + _h(
                    det["aggregate_shortage_on_matching"]) + "</small>"
        link = f'<a href="/ui/batch/tasks/{_h(t["task_id"])}">{_h(t["task_id"])}</a>'
        return (
            f'<tr><td>{link}</td><td><code>{_h(t["tenant_id"])}</code></td>'
            f'<td><code>{_h(t["business_key"])}</code></td>'
            f'<td>{_badge(t["state"])}</td>'
            f'<td>{t["machine_count"]} × {_res(t["need"])}</td>'
            f'<td>{_selector(t["selector"])}</td>'
            f'<td>{wait}</td><td>{reason or "<small>-</small>"}</td>'
            f'<td>{_actions(t)}</td></tr>')

    queue_rows = "".join(_task_row(t, show_wait=True) for t in data["queue"]) \
        or '<tr><td colspan="9">队列为空</td></tr>'
    running_rows = "".join(_task_row(t, show_wait=False)
                           for t in data["running"]) \
        or '<tr><td colspan="9">无运行中任务</td></tr>'
    terminal_rows = "".join(
        f'<tr><td><a href="/ui/batch/tasks/{_h(t["task_id"])}">'
        f'{_h(t["task_id"])}</a></td>'
        f'<td><code>{_h(t["tenant_id"])}</code></td>'
        f'<td><code>{_h(t["business_key"])}</code></td>'
        f'<td>{_badge(t["state"])}</td><td>{_h(t["finish_reason"])}</td>'
        f'<td>{_ts(t["finished_ts"])}</td></tr>'
        for t in data["recent_terminal"]
    ) or '<tr><td colspan="6">暂无终态任务</td></tr>'

    passes = "".join(
        f'<li><code>{_h(p["pass_id"])}</code> {_ts(p["ts"])} · '
        f'考虑 {p["considered"]} · 启动 {len(p["started"])}'
        + (f' · 过期取消 {len(p["expired"])}' if p["expired"] else "")
        + '<br><small>'
        + "；".join(
            f'{_h(o["tenant_id"])}/{_h(o["task_id"][:13])} '
            f'得分 {o["score"]:.3f} → '
            + ('<b style="color:#0a7d2c">启动</b>'
               if o["decision"] == "started"
               else f'<span style="color:#b26a00">推迟:{_h(o["reason"])}</span>')
            for o in p["order"][:12])
        + '</small></li>'
        for p in reversed(data["passes"][:15])
    ) or "<li>暂无调度轮次</li>"

    return f"""<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>多租户批任务接纳器</title><style>{_CSS}</style></head><body>
<h1>多租户批任务接纳器</h1>
<p class="small">
权重公平虚拟时间 + 老化 (等候 &gt; {data["aging_after"]:g}s 起每秒得分
-{data["aging_rate"]:g}) · 成组需求全有或全无 · 资源不足不留部分占用、
不阻挡其他租户 · 2s 自动刷新 ·
<a href="/api/batch/dashboard">JSON</a> ·
<a href="/">作业服务首页</a></p>

<h2>提交 / 登记</h2>
<form method="post" action="/ui/batch/forms/tenant">
租户 <input name="tenant_id" placeholder="tenant-a" required>
权重 <input name="weight" value="1" size="4">
<button>登记租户</button></form>
<form method="post" action="/ui/batch/forms/machine">
机器 <input name="machine_id" placeholder="node-1" required>
CPU <input name="cpu" value="8" size="3">
内存 <input name="mem" value="32" size="4">
GPU <input name="gpu" value="0" size="3">
标签 <input name="labels" placeholder="gpu=a100,zone=hz" size="16">
<button>登记机器</button></form>
<form method="post" action="/ui/batch/forms/submit">
租户 <input name="tenant_id" placeholder="tenant-a" required>
业务键 <input name="business_key" placeholder="run-2026-1" required>
机器数 <input name="machine_count" value="2" size="3">
每台 CPU <input name="cpu" value="4" size="3">
内存 <input name="mem" value="8" size="4">
GPU <input name="gpu" value="1" size="3">
标签 <input name="selector" placeholder="gpu=a100" size="12">
截止秒 <input name="deadline_seconds" placeholder="可选" size="6">
<button>提交任务</button></form>
<form method="post" action="/api/batch/scheduler/tick">
<button>立即调度一轮</button></form>

<h2>集群容量</h2>
<table><tr><th>CPU</th><th>内存</th><th>加速器(GPU)</th></tr>
<tr><td>{_bar("cpu")}</td><td>{_bar("mem")}</td><td>{_bar("gpu")}</td></tr></table>

<h2>租户权重与公平得分</h2>
<table><tr><th>租户</th><th>权重</th><th>已接纳</th><th>排队</th><th>运行</th>
<th>最久等候</th><th>当前调度得分</th></tr>{tenant_rows}</table>

<h2>异构机器</h2>
<table><tr><th>机器</th><th>状态</th><th>总量</th><th>已用</th><th>空闲</th>
<th>标签</th><th>占用数</th></tr>{machine_rows}</table>

<h2>等候队列（按公平次序由调度轮次解释）</h2>
<table><tr><th>任务</th><th>租户</th><th>业务键</th><th>状态</th>
<th>成组需求</th><th>机器选择器</th><th>等候</th><th>拒绝/推迟缘由</th><th>操作</th>
</tr>{queue_rows}</table>

<h2>运行中</h2>
<table><tr><th>任务</th><th>租户</th><th>业务键</th><th>状态</th>
<th>成组需求</th><th>选择器</th><th>等候</th><th>缘由</th><th>操作</th>
</tr>{running_rows}</table>

<h2>最近终态</h2>
<table><tr><th>任务</th><th>租户</th><th>业务键</th><th>状态</th>
<th>原因</th><th>结束时间</th></tr>{terminal_rows}</table>

<h2>最近调度轮次（可解释次序）</h2>
<ul style="font-size:13px">{passes}</ul>
</body></html>"""


def _actions(t: dict) -> str:
    if t["state"] == "QUEUED":
        return ('<form method="post" '
                f'action="/api/batch/tasks/{_h(t["task_id"])}/cancel">'
                '<button>撤回</button></form>')
    if t["state"] == "RUNNING":
        return ('<form method="post" '
                f'action="/api/batch/tasks/{_h(t["task_id"])}/terminate">'
                '<button>终止并归还</button></form>')
    return ""


def render_task_page(data: dict) -> str:
    alloc_rows = "".join(
        f'<tr><td><code>{_h(v["machine_id"])}</code></td>'
        f'<td>{_res(v["allocation"])}</td>'
        f'<td>{("找到" if v["found"] else "机器已注销")}</td>'
        f'<td>{_res(v["machine"]["total"]) if v["machine"] else "-"}'
        f'</td><td>{_res(v["machine"]["free"]) if v["machine"] else "-"}'
        f'</td></tr>'
        for v in data.get("machine_views", [])
    ) or '<tr><td colspan="5">尚未分配（仍在排队）</td></tr>'

    timeline = []
    for e in data["timeline"]:
        kind = e["kind"]
        detail = e.get("detail") or {}
        text = ""
        if kind == "submitted":
            seq = detail.get("enqueue_seq")
            text = ("入队" + (f"，序号 {seq}" if seq is not None else "")
                    + f'，指纹 <code>{_h(data["content_digest"])}</code>')
        elif kind == "deferred":
            text = ("推迟: " + _reason(detail.get("reason"))
                    + f' <small>{_h(detail.get("currently_fitting_machines"))}'
                      f'/{_h(detail.get("required_machines"))} 台此刻可放下'
                      f'</small>')
            sh = detail.get("aggregate_shortage_on_matching")
            if sh:
                text += f' <small>缺口 {_h(sh)}</small>'
        elif kind == "allocated":
            text = ("成组分配并启动 → "
                    + ", ".join(f'<code>{_h(m)}</code>'
                                for m in detail.get("machines", []))
                    + f"，每台 {_res(detail.get('need', {}))}")
        elif kind in ("completed", "failed", "cancelled", "terminated"):
            rel = detail.get("released") or []
            text = (f'{_badge(kind.upper())} 原因 '
                    f'<code>{_h(detail.get("reason"))}</code> · '
                    f'归还 {len(rel)} 台机器容量')
        color = _STATE_COLORS.get(kind, "#444")
        timeline.append(
            f'<tr><td>{_ts(e["ts"])}</td>'
            f'<td><span style="color:{color}">●</span> {_h(kind)}</td>'
            f'<td>{text}</td></tr>')
    timeline_rows = "".join(reversed(timeline)) or \
        '<tr><td colspan="3">无</td></tr>'

    defer = data.get("last_defer")
    if defer:
        reason_box = (
            '<h2>最近拒绝/推迟缘由</h2>'
            f'<p>{_reason(defer.get("code"))}</p><pre>'
            + _h(_pretty(defer.get("detail"))) + "</pre>")
    else:
        reason_box = "<h2>最近拒绝/推迟缘由</h2><p><small>无</small></p>"

    return f"""<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>任务 {_h(data["task_id"])}</title><style>{_CSS}</style></head><body>
<h1>任务 <code>{_h(data["task_id"])}</code> {_badge(data["state"])}</h1>
<p class="small"><a href="/ui/batch">← 返回批接纳总览</a> ·
<a href="/api/batch/tasks/{_h(data["task_id"])}">JSON</a></p>
<table>
<tr><th>租户</th><td><code>{_h(data["tenant_id"])}</code></td></tr>
<tr><th>业务键</th><td><code>{_h(data["business_key"])}</code></td></tr>
<tr><th>内容指纹</th><td><code>{_h(data["content_digest"])}</code></td></tr>
<tr><th>成组需求</th><td>{data["machine_count"]} 台机器协同,
每台 {_res(data["need"])}</td></tr>
<tr><th>机器选择器</th><td>{_selector(data["selector"])}</td></tr>
<tr><th>入队/启动/结束</th><td>{_ts(data["enqueued_ts"])} /
{_ts(data["started_ts"])} / {_ts(data["finished_ts"])}</td></tr>
<tr><th>终态原因</th><td>{_h(data["finish_reason"]) or "-"}</td></tr>
</table>
{_actions_box(data)}
<h2>分配去向（全部容量同时取得后才启动）</h2>
<table><tr><th>机器</th><th>占用容量</th><th>状态</th>
<th>机器总量</th><th>当前空闲</th></tr>{alloc_rows}</table>
{reason_box}
<h2>容量变化 / 生命周期时间线</h2>
<table><tr><th>时间</th><th>事件</th><th>明细</th></tr>
{timeline_rows}</table>
</body></html>"""


def _actions_box(data: dict) -> str:
    if data["state"] == "QUEUED":
        return ('<form method="post" '
                f'action="/api/batch/tasks/{_h(data["task_id"])}/cancel">'
                '<button>撤回排队项</button></form>'
                '<form method="post" '
                f'action="/api/batch/tasks/{_h(data["task_id"])}/release">'
                '<button>归还资源</button></form>')
    if data["state"] == "RUNNING":
        return ('<form method="post" '
                f'action="/api/batch/tasks/{_h(data["task_id"])}/terminate">'
                '<button>终止运行项并归还容量</button></form>')
    return ""


def _pretty(obj) -> str:
    import json
    if obj is None:
        return ""
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
