import uuid
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List

from pydantic import BaseModel, Field

from data_flow.enum_data import ResultStatus


class Result(BaseModel, ABC):

    @abstractmethod
    def get_data(self, key: str) -> Any:
        raise NotImplementedError("子类必须实现 get_data 方法")

    @abstractmethod
    def set_data(self, key: str, data: Any):
        raise NotImplementedError("子类必须实现 set_data 方法")

    @abstractmethod
    def get_result_data(self) -> Any:
        raise NotImplementedError("子类必须实现 get_result_data 方法")


class ExecuteResult(Result, ABC):
    result_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="结果唯一标识")
    node_id: str = Field(..., description="关联的节点ID")
    timestamp: datetime = Field(default_factory=datetime.now, description="执行时间戳")
    success: Optional[bool] = Field(default=False, description="执行是否成功")
    error: Optional[str] = Field(default=None, description="错误信息")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据（如执行时间、重试次数等）")
    output_data: Dict[str, Any] = Field(default_factory=dict, description="输出数据，键为端口ID")

    def get_output(self, port_id: Optional[str] = None) -> Any:
        self.get_data(port_id)

    def set_output(self, port_id: str, data: Any) -> None:
        self.set_data(port_id, data)

    def mark_success(self) -> None:
        """标记执行成功"""
        self.success = True
        self.error = None

    def mark_failure(self, error: str) -> None:
        """标记执行失败并记录错误信息"""
        self.success = False
        self.error = error
        # 可以在这里添加错误追踪相关的元数据
        self.metadata['failure_count'] = self.metadata.get('failure_count', 0) + 1


class DefaultExecuteResult(ExecuteResult):
    """默认执行结果实现类"""

    def get_data(self, key: str) -> Any:
        """获取输出数据"""
        if key is None:
            return self.output_data
        return self.output_data.get(key)

    def set_data(self, key: str, data: Any):
        """设置输出数据"""
        self.output_data[key] = data

    def get_result_data(self) -> Any:
        return self.output_data


class BatchExecuteResult(ExecuteResult):
    """批处理执行结果实现类，支持多条数据的执行结果"""

    # 扩展批处理特有的字段
    batch_size: int = Field(default=0, description="批处理大小")
    processed_count: int = Field(default=0, description="已处理数量")
    batch_results: List[Dict[str, Any]] = Field(default_factory=list, description="每条数据的处理结果")

    def get_output(self, port_id: Optional[str] = None) -> Any:
        """获取批处理输出数据"""
        if port_id is None:
            return self.output_data

        # 对于批处理，返回指定端口的所有数据列表
        return [item.get(port_id) for item in self.batch_results]

    def set_output(self, data: Any, port_id: str) -> None:
        """设置批处理输出数据"""
        self.output_data[port_id] = data
        self.batch_results.append({port_id: data})
        self.processed_count += 1
        self.batch_size = max(self.batch_size, self.processed_count)

    def get_data(self, key: str) -> Any:
        self.get_output(key)

    def set_data(self, key: str, data: Any):
        self.set_output(key, data)

    def get_result_data(self) -> Any:
        return self.output_data


def test():
    # 创建普通执行结果
    result = DefaultExecuteResult(node_id="filter_node_123")
    try:
        # 执行节点逻辑
        filtered_data = [1, 2, 3]
        result.set_output(filtered_data, "passed")
        result.mark_success()
    except Exception as e:
        result.mark_failure(str(e))

    # 获取结果
    if result.success:
        print("输出数据:", result.get_output("passed"))
    else:
        print("执行失败:", result.error)

    # 创建批处理执行结果
    batch_result = BatchExecuteResult(node_id="batch_processor_456")
    for item in [10, 20, 30]:
        batch_result.set_output(item * 2, "processed")
    batch_result.mark_success()

    print(f"批处理完成: 共{batch_result.batch_size}条，处理{batch_result.processed_count}条")


if __name__ == '__main__':
    test()
