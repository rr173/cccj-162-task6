"""多租户成组批任务接纳器 (multi-tenant gang admission)。

与既有抢占/检查点恢复子系统 *完全独立*:
  * 复用同一条顺序化事件日志 (persistence.Journal, 全局单调 seq), 但事件
    类型全部以 ``adm_`` 前缀区分, 由 AdmissionManager 自己重放, 既有
    JobManager 的重放与作业恢复行为不发生任何变化;
  * 自己的锁、状态机与 HTTP 路由 (/api/admission/*, /ui/admission/*)。

核心不变量见各模块文档。
"""

from .manager import AdmissionManager
from .model import (
    QUEUED,
    ALLOCATED,
    RUNNING,
    SUCCEEDED,
    TERMINATED,
    CANCELLED,
    REJECTED,
    ACTIVE_TASK_STATES,
    RUNNABLE_TASK_STATES,
    TERMINAL_TASK_STATES,
)

__all__ = [
    "AdmissionManager",
    "QUEUED",
    "ALLOCATED",
    "RUNNING",
    "SUCCEEDED",
    "TERMINATED",
    "CANCELLED",
    "REJECTED",
    "ACTIVE_TASK_STATES",
    "RUNNABLE_TASK_STATES",
    "TERMINAL_TASK_STATES",
]
