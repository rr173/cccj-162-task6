"""批接纳器领域错误 (与 hpc_cr.errors 独立, 稳定 reason 码)。"""

from __future__ import annotations


class BatchError(Exception):
    http_status = 500
    error_code = "batch_internal_error"

    def __init__(self, message: str = "", *, reason: str | None = None):
        super().__init__(message)
        # 稳定机器可读原因码: 页面与客户端据此展示"拒绝缘由"
        self.reason = reason

    def to_dict(self) -> dict:
        body = {"error": self.error_code, "message": str(self)}
        if self.reason:
            body["reason"] = self.reason
        return body


class BatchValidationError(BatchError):
    http_status = 400
    error_code = "batch_validation_error"


class BatchNotFound(BatchError):
    http_status = 404
    error_code = "batch_not_found"


class BatchConflict(BatchError):
    """状态冲突 / 容量不足 / 业务键冲突。

    reason 细分:
      business_key_conflict  同业务键但内容变化
      insufficient_cluster_capacity  全集群总量都装不下 (接纳期即拒绝)
      not_cancellable / not_terminatable  状态机不允许
    """

    http_status = 409
    error_code = "batch_conflict"
