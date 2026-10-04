"""多租户批任务接纳器 (Multi-tenant gang batch admission)。

与既有抢占/检查点恢复服务相互独立的增量子系统:

* 租户按 *权重 + 等候时长* (加权公平虚拟时间 + 老化) 获得可解释的执行机会;
* 任务为 *成组 (gang)* 需求: 必须一次性从异构机器中拿到全部机器容量才
  启动, 不够则不留任何部分占用, 也不阻挡其他租户此刻可启动的任务;
* 业务键幂等: 同键同内容返回原结果, 同键异内容稳定冲突;
* 事件溯源持久化 (独立 journal), 重启找回排队项与已分配容量;
* 撤回 / 终止 / 归还资源全部幂等, 机器容量账目恰好一次归还。

子系统拥有独立的锁与事件日志 (``<data_dir>/batch/journal.jsonl``),
不触碰既有作业恢复的任何状态与事件流。
"""

from .manager import BatchManager
from .errors import BatchError, BatchConflict, BatchNotFound, BatchValidationError

__all__ = [
    "BatchManager",
    "BatchError",
    "BatchConflict",
    "BatchNotFound",
    "BatchValidationError",
]
