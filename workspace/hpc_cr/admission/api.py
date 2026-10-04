"""接纳器 HTTP 路由 (纯增量)。

所有路径位于 ``/api/admission/*`` 与 ``/ui/admission/*``, 由既有 ApiHandler
在 *未命中任何既有路由* 时委托给本模块 (:func:`route_get` /
:func:`route_post`), 因此既有作业/检查点/GC/迁移 API 与恢复行为完全不变。
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler

from ..errors import CpError
from .manager import AdmissionManager
from .ui import render_machine_detail, render_overview, render_task_detail

API_PREFIX = ["api", "admission"]
UI_PREFIX = ["ui", "admission"]


def send_json(handler: BaseHTTPRequestHandler, status: int, obj) -> None:
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def send_html(handler: BaseHTTPRequestHandler, html_text: str) -> None:
    data = html_text.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _handle(handler, fn, status=200):
    try:
        result = fn()
    except CpError as exc:
        send_json(handler, exc.http_status, exc.to_dict())
        return True
    send_json(handler, status, result if result is not None else {"ok": True})
    return True


# ------------------------------------------------------------------ GET
def route_get(handler, parts: list[str]) -> bool:
    mgr: AdmissionManager = handler.server.admission  # type: ignore[attr-defined]
    if parts[:2] == UI_PREFIX:
        sub = parts[2:]
        if sub == [] or sub == [""]:
            send_html(handler, render_overview(mgr.overview()))
            return True
        if len(sub) == 2 and sub[0] == "tasks":
            task = mgr.get_task(sub[1])
            machines = {m["machine_id"]: m for m in mgr.list_machines()}
            send_html(handler, render_task_detail(task, machines))
            return True
        if len(sub) == 2 and sub[0] == "machines":
            send_html(handler, render_machine_detail(
                _machine_or_404(mgr, sub[1])))
            return True
        return False

    if parts[:2] != API_PREFIX:
        return False
    sub = parts[2:]
    if sub == ["overview"]:
        return _handle(handler, lambda: mgr.overview())
    if sub == ["tenants"]:
        return _handle(handler, lambda: {"tenants": mgr.list_tenants(),
                                         "stats": mgr.tenant_stats()})
    if sub == ["machines"]:
        return _handle(handler, lambda: {"machines": mgr.list_machines()})
    if len(sub) == 2 and sub[0] == "machines":
        return _handle(handler,
                       lambda: _machine_or_404(mgr, sub[1]))
    if sub == ["tasks"]:
        from urllib.parse import parse_qs, urlsplit
        q = {k: v[0] for k, v in
             parse_qs(urlsplit(handler.path).query).items()}
        return _handle(handler, lambda: {"tasks": mgr.list_tasks(
            tenant_id=q.get("tenant_id"), state=q.get("state"))})
    if len(sub) == 2 and sub[0] == "tasks":
        return _handle(handler, lambda: mgr.get_task(sub[1]))
    if sub == ["schedule"]:
        return _handle(handler, lambda: mgr.last_schedule() or {})
    return False


def _machine_or_404(mgr: AdmissionManager, machine_id: str) -> dict:
    for m in mgr.list_machines():
        if m["machine_id"] == machine_id:
            return m
    from ..errors import NotFound
    raise NotFound(f"unknown machine {machine_id}")


# ------------------------------------------------------------------ POST
def route_post(handler, parts: list[str], body: dict) -> bool:
    mgr: AdmissionManager = handler.server.admission  # type: ignore[attr-defined]
    if parts[:2] != API_PREFIX:
        return False
    sub = parts[2:]

    def _int(name, required=True, default=None):
        if name not in body:
            if required:
                raise KeyError(name)
            return default
        return int(body[name])

    if sub == ["tenants"]:
        return _handle(handler, lambda: mgr.upsert_tenant(
            body["tenant_id"], weight=body.get("weight"),
            name=body.get("name")), status=201)

    if sub == ["machines"]:
        return _handle(handler, lambda: mgr.register_machine(
            body["machine_id"],
            cpus=int(body["cpus"]),
            memory_mb=int(body["memory_mb"]),
            accelerators=body.get("accelerators"),
            state=body.get("state", "ONLINE")), status=201)

    if len(sub) == 2 and sub[0] == "machines":
        mid = sub[1]
        action = body.get("action")
        if action == "drain":
            return _handle(handler, lambda: mgr.drain_machine(mid))
        if action == "online":
            return _handle(handler, lambda: mgr.online_machine(mid))
        if action == "offline":
            return _handle(handler, lambda: mgr.offline_machine(mid))
        send_json(handler, 404, {"error": "not_found",
                                "message": f"unknown action {action}"})
        return True

    if sub == ["tasks"]:
        return _handle(handler, lambda: mgr.submit(
            business_key=body["business_key"],
            tenant_id=body["tenant_id"],
            replicas=int(body["replicas"]),
            cpus_per_replica=int(body["cpus_per_replica"]),
            memory_mb_per_replica=int(body["memory_mb_per_replica"]),
            accelerators_per_replica=body.get("accelerators_per_replica"),
            exclusive=bool(body.get("exclusive", False)),
            name=body.get("name"),
            payload=body.get("payload"),
            task_id=body.get("task_id")), status=201)

    if sub == ["tasks", "by-key"]:
        return _handle(handler,
                       lambda: mgr.get_task_by_key(body["business_key"]))

    if len(sub) == 3 and sub[0] == "tasks" and sub[2] == "start":
        return _handle(handler, lambda: mgr.start_task(sub[1]))
    if len(sub) == 3 and sub[0] == "tasks" and sub[2] == "cancel":
        return _handle(handler, lambda: mgr.cancel_task(
            sub[1], reason=body.get("reason", "cancelled_by_submitter")))
    if len(sub) == 3 and sub[0] == "tasks" and sub[2] == "terminate":
        return _handle(handler, lambda: mgr.terminate_task(
            sub[1], reason=body.get("reason", "terminated_by_user")))
    if len(sub) == 3 and sub[0] == "tasks" and sub[2] == "complete":
        return _handle(handler, lambda: mgr.complete_task(
            sub[1], result=body.get("result"),
            status=body.get("status", "succeeded")))

    if sub == ["schedule"]:
        return _handle(handler, lambda: mgr.schedule())

    return False
