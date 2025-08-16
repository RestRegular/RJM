from typing import Tuple

import app.utils.extra_info as ei


class Log:

    def __init__(self, module: str, class_name: str, type_: str, message: str, flow_id: str = None,
                 profile_id: str = None, node_id: str = None, traceback=None):
        if traceback is None:
            traceback = []

        self.message = message
        self.type = type_
        self.class_name = class_name
        self.module = module
        self.node_id = node_id
        self.profile_id = profile_id
        self.flow_id = flow_id
        self.traceback = traceback

    def __repr__(self):
        return f"Node: {self.node_id}, class: {self.class_name}, module: {self.module}, " \
               f"type: {self.type}, message: {self.message}"

    def to_extra(self) -> dict:
        return ei.exact(
            origin="workflow",
            class_name=self.class_name,
            package=self.module,
            flow_id=self.flow_id,
            node_id=self.node_id
        )

    def is_error(self) -> bool:
        return self.type == 'error'

    def is_waring(self) -> bool:
        return self.type == 'warning'

    def is_debug(self) -> bool:
        return self.type == 'debug'

    def is_info(self) -> bool:
        return self.type == 'info'


class ConsoleStatus:
    errors = 0
    warnings = 0
    infos = 0


class Console:

    def __init__(self, class_name, module, flow_id=None, profile_id=None, node_id=None):
        self.module = module
        self.class_name = class_name
        self.debugs = []
        self.infos = []
        self.errors = []
        self.warnings = []
        self.node_id = node_id
        self.flow_id = flow_id
        self.profile_id = profile_id

    def debug(self, item: str):
        self.debugs.append(item)

    def log(self, item: str):
        self.infos.append(item)

    def error(self, item: str):
        self.errors.append(item)

    def warning(self, item: str):
        self.warnings.append(item)

    def get_logs(self) -> Tuple[Log]:
        for type_, logs in zip(["debug", "info", "warning", "error"], [self.debugs, self.infos, self.warnings, self.errors]):
            for message in logs:
                yield Log(self.module,
                          self.class_name,
                          type_,
                          message,
                          profile_id=self.profile_id,
                          flow_id=self.flow_id,
                          node_id=self.node_id)

    def get_status(self) -> ConsoleStatus:
        status_ = ConsoleStatus()
        status_.errors = len(self.errors)
        status_.warnings = len(self.warnings)
        status_.infos = len(self.infos)
        return status_

    def __repr__(self):
        return f"Class: {self.class_name}, Errors: {self.errors}, Warnings: {self.warnings}, Infos: {self.infos}"

    def dict(self):
        return {
            "infos": self.infos,
            "errors": self.errors,
            "warnings": self.warnings
        }

    def append(self, data):
        self.infos = data['infos']
        self.errors = data['errors']
        self.warnings = data['warnings']


# 模拟工作流执行
if __name__ == "__main__":
    # 假设在一个名为 "order_processing" 的模块中，有一个 "PaymentNode" 类负责处理支付
    class PaymentNode:
        def __init__(self, flow_id, node_id, profile_id):
            # 初始化日志收集器，关联当前类、模块和上下文
            self.console = Console(
                class_name="PaymentNode",
                module="order_processing",
                flow_id=flow_id,
                node_id=node_id,
                profile_id=profile_id
            )

        def process_payment(self, amount):
            # 模拟业务逻辑与日志记录
            self.console.debug("开始处理支付流程")

            if amount <= 0:
                self.console.error(f"支付金额错误：{amount}（必须大于0）")
                return False

            if amount > 10000:
                self.console.warning(f"大额支付预警：金额 {amount} 元，建议人工审核")

            self.console.log(f"支付成功，金额：{amount} 元")
            return True

    # 初始化支付节点（关联流程ID、节点ID和用户档案ID）
    payment_node = PaymentNode(
        flow_id="flow_123",
        node_id="node_payment",
        profile_id="user_456"
    )

    # 处理两笔支付
    payment_node.process_payment(5000)  # 正常支付（无错误，有日志）
    payment_node.process_payment(-200)  # 错误支付（金额为负）

    # 1. 获取所有日志并打印
    for log in payment_node.console.get_logs():
        print(log)

    # 2. 获取日志状态统计
    status = payment_node.console.get_status()
    print("\n===== 日志统计 =====")
    print(f"错误数：{status.errors}，警告数：{status.warnings}，信息数：{status.infos}")