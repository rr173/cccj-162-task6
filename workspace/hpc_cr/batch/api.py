"""批接纳器 HTTP 路由: 从主 ApiHandler 分发, 路径前缀 /api/batch 与 /ui/batch。

处理器刻意写成无状态静态函数, 由主 handler 提供请求读写能力, 这样
批接纳子系统完全不需要改动主服务的 BaseHTTPRequestHandler 结构:
主 handler 仅新增两处分发调用。
"""

from __future__ import annotations

import json
import urllib.parse

from ..errors import CpError
from .errors import BatchError
from .manager import BatchManager
from .ui import render_batch_dashboard, render_task_page


class BatchRouter:
    def __init__(self, manager: BatchManager):
        self.manager = manager

    # ---------------------------------------------------------------- GET
    def get(self, handler, parts: list[str], query: dict[str, str]):
        m = self.manager
        try:
            if parts == ["api", "batch", "dashboard"]:
                return handler._send_json(200, m.dashboard_data())
            if parts == ["api", "batch", "tenants"]:
                return handler._send_json(200, {"tenants": m.list_tenants()})
            if parts == ["api", "batch", "machines"]:
                return handler._send_json(200, {"machines": m.list_machines()})
            if parts == ["api", "batch", "tasks"]:
                return handler._send_json(200, {"tasks": m.list_tasks(
                    tenant_id=query.get("tenant_id"),
                    state=query.get("state"))})
            if len(parts) == 4 and parts[:3] == ["api", "batch", "tasks"]:
                return handler._send_json(200, m.get_task(parts[3]))
            if parts == ["api", "batch", "passes"]:
                return handler._send_json(200, {"passes": m.list_passes()})
            if parts == ["ui", "batch"]:
                return handler._html(render_batch_dashboard(m.dashboard_data()),
                                     handler.headers.get("Accept", ""))
            if len(parts) == 4 and parts[:3] == ["ui", "batch", "tasks"]:
                return handler._html(
                    render_task_page(m.task_page_data(parts[3])),
                    handler.headers.get("Accept", ""))
        except BatchError as exc:
            handler._send_json(exc.http_status, exc.to_dict())
            return True
        except CpError as exc:
            handler._send_json(exc.http_status, exc.to_dict())
            return True
        return False

    # --------------------------------------------------------------- POST
    def post(self, handler, parts: list[str], body: dict):
        m = self.manager
        try:
            if parts == ["api", "batch", "tenants"]:
                result = m.register_tenant(
                    body["tenant_id"], weight=float(body.get("weight", 1.0)))
                return handler._send_json(201, result)
            if (len(parts) == 4 and parts[:3] == ["api", "batch", "tenants"]):
                result = m.update_tenant(parts[3], float(body.get("weight")))
                return handler._send_json(200, result)

            if parts == ["api", "batch", "machines"]:
                result = m.add_machine(
                    body["machine_id"],
                    {"cpu": body.get("cpu", 0), "mem": body.get("mem", 0),
                     "gpu": body.get("gpu", 0)},
                    labels=_parse_labels(body.get("labels")))
                return handler._send_json(201, result)
            if parts == ["api", "batch", "machines", "state"]:
                result = m.set_machine_state(body["machine_id"],
                                             body["state"])
                return handler._send_json(200, result)
            if (len(parts) == 4
                    and parts[:3] == ["api", "batch", "machines"]):
                result = m.update_machine(
                    parts[3],
                    total=(
                        {"cpu": body.get("cpu"), "mem": body.get("mem"),
                         "gpu": body.get("gpu")}
                        if any(k in body for k in ("cpu", "mem", "gpu"))
                        else None),
                    labels=(body["labels"]
                            if isinstance(body.get("labels"), dict)
                            else _labels_optional(body.get("labels"))))
                return handler._send_json(200, result)

            if parts == ["api", "batch", "tasks"]:
                result = m.submit_task(
                    tenant_id=body["tenant_id"],
                    business_key=body["business_key"],
                    machine_count=int(body.get("machine_count", 1)),
                    need=body.get("need") or {
                        "cpu": body.get("cpu", 0),
                        "mem": body.get("mem", 0),
                        "gpu": body.get("gpu", 0)},
                    selector=body.get("selector")
                    if isinstance(body.get("selector"), dict)
                    else _parse_labels(body.get("selector")),
                    task_id=body.get("task_id"),
                    deadline_seconds=body.get("deadline_seconds"))
                return handler._send_json(201, result)
            if len(parts) == 5 and parts[:3] == ["api", "batch", "tasks"]:
                tid, action = parts[3], parts[4]
                if action == "cancel":
                    result = m.cancel_task(
                        tid, reason=body.get("reason", "cancelled_by_user"))
                elif action == "terminate":
                    result = m.terminate_task(
                        tid, reason=body.get("reason", "terminated_by_user"))
                elif action == "complete":
                    result = m.complete_task(tid, result=body.get("result"))
                elif action == "fail":
                    result = m.fail_task(
                        tid, reason=body.get("reason", "failed"),
                        result=body.get("result"))
                elif action == "release":
                    result = m.release_resources(tid)
                else:
                    return False
                return handler._send_json(200, result)
            if parts == ["api", "batch", "scheduler", "tick"]:
                return handler._send_json(200, m.run_scheduler_pass())

            # 页面表单 (application/x-www-form-urlencoded)
            if len(parts) == 4 and parts[:3] == ["ui", "batch", "forms"]:
                self._form_action(parts[3], body)
                handler._redirect("/ui/batch")
                return True
        except BatchError as exc:
            handler._send_json(exc.http_status, exc.to_dict())
            return True
        except CpError as exc:  # 与主服务错误形态保持一致
            handler._send_json(exc.http_status, exc.to_dict())
            return True
        except (KeyError, TypeError, ValueError) as exc:
            handler._send_json(400, {"error": "batch_validation_error",
                                     "message": f"bad request field: {exc}"})
            return True
        return False

    # ------------------------------------------------------------ forms
    def _form_action(self, name: str, form: dict) -> None:
        m = self.manager
        if name == "tenant":
            m.register_tenant(form["tenant_id"],
                              weight=float(form.get("weight") or 1.0))
        elif name == "machine":
            m.add_machine(
                form["machine_id"],
                {"cpu": int(form.get("cpu") or 0),
                 "mem": int(form.get("mem") or 0),
                 "gpu": int(form.get("gpu") or 0)},
                labels=_parse_labels(form.get("labels")))
        elif name == "submit":
            m.submit_task(
                tenant_id=form["tenant_id"],
                business_key=form["business_key"],
                machine_count=int(form.get("machine_count") or 1),
                need={"cpu": int(form.get("cpu") or 0),
                      "mem": int(form.get("mem") or 0),
                      "gpu": int(form.get("gpu") or 0)},
                selector=_parse_labels(form.get("selector")),
                deadline_seconds=(float(form["deadline_seconds"])
                                  if form.get("deadline_seconds") else None))
        else:
            from .errors import BatchNotFound
            raise BatchNotFound(f"unknown form action {name}")


def _parse_labels(raw) -> dict[str, str]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    labels = {}
    for pair in str(raw).split(","):
        pair = pair.strip()
        if not pair:
            continue
        if "=" not in pair:
            from .errors import BatchValidationError
            raise BatchValidationError(
                "labels must be key=value pairs separated by commas")
        k, v = pair.split("=", 1)
        labels[k.strip()] = v.strip()
    return labels


def _labels_optional(raw):
    if raw is None:
        return None
    return _parse_labels(raw)


def read_request_body(handler) -> dict:
    """读取 JSON 或表单 body; 表单解析为扁平 dict。"""
    length = int(handler.headers.get("Content-Length") or 0)
    raw = handler.rfile.read(length) if length else b""
    ctype = handler.headers.get("Content-Type", "")
    if not raw:
        return {}
    if "application/json" in ctype or raw[:1] in (b"{", b"["):
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as exc:
            from .errors import BatchValidationError
            raise BatchValidationError(f"invalid JSON body: {exc}") from exc
        if not isinstance(body, dict):
            from .errors import BatchValidationError
            raise BatchValidationError("request body must be a JSON object")
        return body
    parsed = urllib.parse.parse_qs(raw.decode("utf-8"))
    return {k: v[-1] for k, v in parsed.items()}
