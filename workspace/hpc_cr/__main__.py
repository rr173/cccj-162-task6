"""``python -m hpc_cr`` 启动 HTTP 服务。"""

import argparse

from .api import serve_forever


def main() -> None:
    ap = argparse.ArgumentParser(description="HPC 抢占/检查点恢复服务")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--data-dir", default=None,
                    help="持久化数据目录 (事件日志/blob/签名密钥); "
                         "不给定则为纯内存模式, 重启不保留状态。")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--no-batch", dest="enable_batch", action="store_false",
                    help="禁用多租户批任务接纳器 (默认启用, 独立日志)")
    args = ap.parse_args()
    serve_forever(host=args.host, port=args.port, verbose=args.verbose,
                  data_dir=args.data_dir, enable_batch=args.enable_batch)


if __name__ == "__main__":
    main()
