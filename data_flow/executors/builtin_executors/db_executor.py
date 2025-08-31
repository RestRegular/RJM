import asyncio
import logging

import aiomysql
from typing import Dict, Any, List, Optional

from pydantic import BaseModel

from data_flow import ExecutionContext
from data_flow.node import Node
from data_flow.node_config import NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory
from data_flow.result import DefaultExecuteResult, ExecuteResult

__all__ = ["DBReadConfig", "DBWriteConfig", "DBConnectionConfig", "DBReadExecutor", "DBWriteExecutor"]

from utils.log_system import get_logger


class DBConnectionConfig(BaseModel):
    host: str = "localhost"
    port: int = 3306
    user: str
    password: str
    dbname: str

    def __str__(self):
        config = self.model_dump()
        config["password"] = "******"
        return str(config)

    def __repr__(self):
        return str(self)


class DBReadConfig(NodeConfig):
    """数据库读取 节点配置（支持多端口多查询）"""
    db_conn: DBConnectionConfig  # 数据库连接信息（host, port, user, password, dbname等）
    port_query_mapping: Dict[str, str]  # 端口ID与SQL查询语句的映射关系
    batch_size: int = 1000  # 批量读取大小

    def __init__(self, db_conn: DBConnectionConfig,
                 port_query_mapping: Dict[str, str],
                 batch_size: int = 1000,
                 **kwargs):
        super().__init__(db_conn=db_conn,
                         port_query_mapping=port_query_mapping,
                         batch_size=batch_size,
                         **kwargs)


@NodeExecutorFactory.register_executor
class DBReadExecutor(NodeExecutor):
    """数据库读取节点执行器，支持多端口多查询的数据库读取操作"""

    logger = get_logger(f"{__name__}.DBReadExecutor")

    async def execute(self, **kwargs) -> ExecuteResult:
        self.process_args(**kwargs)
        _ = self.get_input_data()

        results: Dict[str, List[Any]] = {}

        # 获取数据库连接配置和端口-查询映射关系
        db_conn: DBConnectionConfig = self.node.get_config("db_conn")
        port_query_mapping: Dict[str, str] = self.node.get_config("port_query_mapping")

        # 验证必要配置
        if not db_conn or not port_query_mapping:
            self.log_validation_failed(Exception("缺少必要的配置：数据库连接信息或端口查询映射未设置"),
                                       "缺少必要的配置：数据库连接信息或端口查询映射未设置")
            return self.generate_default_execute_result(
                success=False,
                error="缺少必要的配置：数据库连接信息或端口查询映射未设置"
            )

        # 建立数据库连接
        conn = None
        self.log_handle_start()
        try:
            conn = await aiomysql.connect(
                host=db_conn.host or "localhost",  # 提供默认主机
                port=db_conn.port or 3306,  # 提供默认端口
                user=db_conn.user,
                password=db_conn.password,
                db=db_conn.dbname,
                autocommit=True
            )

            async with conn.cursor() as cursor:
                for port_id, query in port_query_mapping.items():
                    try:
                        # 执行查询
                        await cursor.execute(query)
                        # 获取所有结果行
                        rows = await cursor.fetchall()
                        # 获取列信息（字段名）
                        columns = [desc[0] for desc in cursor.description]
                        # 转换为字典列表：将每行数据与列名映射
                        result = [dict(zip(columns, row)) for row in rows]
                        results[port_id] = result
                        self.log_info(f"执行SQL {query}，执行结果：读取到 {len(result)} 条数据")
                    except Exception as e:
                        self.log_execution_failed(e, f"端口 {port_id} 执行查询 {query} 失败: {str(e)}")
                        return self.generate_default_execute_result(
                            success=False,
                            error=f"端口 {port_id} 执行查询失败: {str(e)}"
                        )
            return DefaultExecuteResult(
                node_id=self.node.id,
                success=True,
                output_data=results
            )
        except Exception as e:
            self.log_execution_failed(e, f"数据库连接或执行失败: {str(e)}")
            return self.generate_default_execute_result(
                success=False,
                error=f"数据库连接或执行失败: {str(e)}"
            )
        finally:
            if conn:
                conn.close()

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.DB_READ

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> DBReadConfig:
        """提供节点默认配置"""
        return DBReadConfig(
            db_conn=DBConnectionConfig(
                host="localhost",
                port=3306,
                user="",
                password="",
                dbname=""
            ),
            port_query_mapping={}
        )

    def get_logger(self) -> logging.Logger:
        return self.logger


class DBWriteConfig(NodeConfig):
    """数据库写入节点配置"""
    default_table: str = ""  # 默认表名
    db_conn: DBConnectionConfig  # 数据库连接信息
    batch_size: int = 1000  # 批量写入大小
    upsert_key: Optional[str] = None  # 用于upsert的关键字段
    port_table_mapping: Dict[str, str] = {}  # 输入端口与表名的映射

    def __init__(self, db_conn: DBConnectionConfig,
                 default_table: str = "", batch_size: int = 1000,
                 upsert_key: Optional[str] = None,
                 port_table_mapping: Dict[str, str] = None, **kwargs):
        super().__init__(default_table=default_table, db_conn=db_conn, batch_size=batch_size,
                         upsert_key=upsert_key, port_table_mapping=port_table_mapping,
                         **kwargs)


@NodeExecutorFactory.register_executor
class DBWriteExecutor(NodeExecutor):
    """数据库写入节点执行器"""

    logger = get_logger(f"{__name__}.DBWriteExecutor")

    async def execute(self, **kwargs) -> ExecuteResult:
        self.process_args(**kwargs)
        input_data: Dict[str, Any] = self.get_input_data()
        if not input_data:
            error = ValueError("所有输入端口数据均为空")
            self.log_validation_failed(error, "所有输入端口数据均为空")
            raise error
        # 获取配置
        config = self.node.config
        db_conn = config.db_conn
        batch_size = config.batch_size or 100
        # 建立数据库连接
        conn = None
        self.log_handle_start()
        try:
            conn = await aiomysql.connect(
                host=db_conn.host,
                port=int(db_conn.port or 3306),
                user=db_conn.user,
                password=db_conn.password,
                db=db_conn.dbname,
                autocommit=True
            )
            async with conn.cursor() as cursor:
                result_stats = {}
                # 遍历每个端口的数据并处理
                for port_id, data in input_data.items():
                    # 验证数据格式
                    if not data or not isinstance(data, list):
                        error = ValueError(f"端口[{port_id}]的数据必须为非空列表")
                        self.log_validation_failed(error, f"端口[{port_id}]的数据必须为非空列表: {str(data) + ('...' if len(str(data)) > 50 else '')}")
                        raise error
                    # 确定目标表名（优先使用映射，其次使用默认表名）
                    table = config.port_table_mapping.get(port_id, None)
                    if not table:
                        table  = config.default_table
                        if not table:
                            error = ValueError(f"端口[{port_id}]未配置目标数据表且未指定默认数据表")
                            self.log_validation_failed(error, f"端口[{port_id}]未配置目标数据表且未指定默认数据表")
                            raise error
                        self.get_logger().warning(f"端口[{port_id}]未配置目标数据表，将自动使用默认数据表[{table}]")
                    # 生成SQL并执行
                    fields = data[0].keys()
                    placeholders = ", ".join([f"%({k})s" for k in fields])
                    sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"
                    # 处理更新逻辑
                    if config.upsert_key:
                        update_clause = ", ".join([
                            f"{k}=VALUES({k})" for k in fields
                            if k != config.upsert_key
                        ])
                        sql += f" ON DUPLICATE KEY UPDATE {update_clause}"
                    # 批量写入
                    total_written = 0
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        total_written += await cursor.executemany(sql, batch)
                    self.log_info(f"执行SQL: {sql}, 执行结果: {total_written}条记录已写入[{table}]")
                    result_stats[port_id] = {
                        "table": table,
                        "written_count": total_written
                    }
                conn.close()
                return self.generate_default_execute_result(
                    result_data={"port_stats": result_stats,
                                 "total": sum(v["written_count"] for v in result_stats.values())}
                )
        except Exception as e:
            if conn:
                conn.close()
            self.log_execution_failed(e, str(e))
            return self.generate_default_execute_result(
                success=False,
                error=str(e)
            )

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.DB_WRITE

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> NodeConfig:
        return None

    def get_logger(self) -> logging.Logger:
        return self.logger
