import asyncio
import aiomysql
from typing import Dict, Any, List, Optional

from pydantic import BaseModel

from data_flow.node import Node
from data_flow.node_config import NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.result import DefaultExecuteResult, ExecuteResult
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory

__all__ = ["DBReadConfig", "DBWriteConfig", "DBReadExecutor", "DBWriteExecutor"]


class DBReadConfig(NodeConfig):
    """数据库读取节点配置"""
    query: str  # SQL查询语句
    db_conn: Dict[str, str]  # 数据库连接信息（host, port, user, password, dbname等）
    batch_size: int = 1000  # 批量读取大小


class DBWriteConfig(NodeConfig):
    """数据库写入节点配置"""
    table: str  # 目标表名
    db_conn: Dict[str, str]  # 数据库连接信息
    batch_size: int = 1000  # 批量写入大小
    upsert_key: Optional[str] = None  # 用于upsert的关键字段


@NodeExecutorFactory.register_executor
class DBReadExecutor(NodeExecutor):
    """数据库读取节点执行器"""

    async def execute(self, **kwargs) -> ExecuteResult:
        self.process_args(**kwargs)
        node: Node = self.node

        # 获取配置
        query = node.get_config("query")
        db_conn = node.get_config("db_conn")
        batch_size = node.get_config("batch_size")

        # 执行查询
        conn = await aiomysql.connect(
            host=db_conn["host"],
            port=int(db_conn.get("port", 3306)),
            user=db_conn["user"],
            password=db_conn["password"],
            db=db_conn["dbname"],
            autocommit=True
        )

        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query)
                result = await cursor.fetchmany(batch_size)

                # 准备输出
                output_port_id = node.outputs[0].id if node.outputs else "data"
                conn.close()
                return DefaultExecuteResult(
                    node_id=node.id,
                    output_data={output_port_id: result},
                    success=True
                )
        except Exception:
            conn.close()

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.DB_READ


@NodeExecutorFactory.register_executor
class DBWriteExecutor(NodeExecutor):
    """数据库写入节点执行器"""

    async def execute(self, **kwargs) -> ExecuteResult:
        self.process_args(**kwargs)
        # 获取输入数据
        input_data = self.node.get_config("data.input")
        # 获取输入数据
        input_port_id = next(
            (port.id for port in self.node.inputs if port.required),
            self.node.inputs[0].id if self.node.inputs else None
        )

        if not input_port_id or input_port_id not in input_data:
            raise ValueError(f"数据库写入节点 {self.node.id} 缺少必要的输入数据")

        input_data = input_data[input_port_id]
        if not input_data or not isinstance(input_data, list):
            raise ValueError("写入数据必须为非空列表")

        # 获取配置
        table = self.node.get_config("table")
        db_conn = self.node.get_config("db_conn")
        batch_size = self.node.get_config("batch_size")
        upsert_key = self.node.get_config("upsert_key")

        # 执行写入
        conn = await aiomysql.connect(
            host=db_conn["host"],
            port=int(db_conn.get("port", 3306)),
            user=db_conn["user"],
            password=db_conn["password"],
            db=db_conn["dbname"],
            autocommit=True
        )

        try:
            async with conn.cursor() as cursor:
                # 生成插入SQL（简化版，实际应处理字段映射）
                fields = input_data[0].keys()
                placeholders = ", ".join([f"%({k})s" for k in fields])
                sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"

                if upsert_key:
                    update_clause = ", ".join([f"{k}=VALUES({k})" for k in fields if k != upsert_key])
                    sql += f" ON DUPLICATE KEY UPDATE {update_clause}"

                # 批量写入
                total_written = 0
                for i in range(0, len(input_data), batch_size):
                    batch = input_data[i:i + batch_size]
                    total_written += await cursor.executemany(sql, batch)

                return DefaultExecuteResult(
                    node_id=self.node.id,
                    output_data={"written_count": total_written},
                    success=True
                )
        except Exception as e:
            conn.close()
            return DefaultExecuteResult(
                node_id=node.id,
                success=False,
                error=str(e)
            )

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.DB_WRITE
