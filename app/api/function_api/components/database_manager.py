#
# Created by RestRegular on 2025/7/18
#
import threading
import time
from typing import Dict, Any, List, Optional, Union, Set, Callable
import json
import mysql.connector
from mysql.connector import Error
from redis import Redis
from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer, TopicPartition, OffsetAndMetadata
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, DeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import NewTopic


class DataBaseManager:
    """通用数据库操作类，提供对MySQL、Redis和Kafka的CRUD操作"""

    def __init__(self,
                 mysql_config: Optional[Dict[str, str]] = None,
                 redis_config: Optional[Dict[str, Any]] = None,
                 kafka_config: Optional[Dict[str, str]] = None,
                 kafka_producer_config: Optional[Dict[str, Any]] = None):
        """
        初始化数据库处理器

        :param mysql_config: MySQL连接配置
        :param redis_config: Redis连接配置
        :param kafka_config: Kafka连接配置
        """
        # 初始化配置
        self._mysql_config = mysql_config or {}
        self._redis_config = redis_config or {}
        self._kafka_config = kafka_config or {}
        self._kafka_producer_config = kafka_producer_config or {}
        if 'bootstrap_servers' not in self._kafka_producer_config:
            self._kafka_producer_config['bootstrap_servers'] = self._kafka_config.get('bootstrap_servers')
        if 'value_serializer' not in self._kafka_producer_config:
            self._kafka_producer_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')

        # 初始化连接对象
        self._mysql_conn = None
        self._redis_client: Optional[Redis] = None
        self._kafka_producer = None
        self._kafka_admin = None
        self._kafka_consumers: Dict[str, KafkaConsumer] = {}  # 存储消费者实例
        self._consume_flags = {}

        # 自动连接
        if self._mysql_config:
            self._connect_mysql()
        if self._redis_config:
            self._connect_redis()
        if self._kafka_config:
            self._connect_kafka()

    def set_mysql_config(self, mysql_config: Optional[Dict[str, str]]):
        self._mysql_config = mysql_config

    def set_redis_config(self, redis_config: Optional[Dict[str, Any]]):
        self._redis_config = redis_config

    def set_kafka_config(self, kafka_config: Optional[Dict[str, str]]):
        self._kafka_config = kafka_config

    def get_mysql_config(self):
        return self._mysql_config

    def get_redis_config(self):
        return self._redis_config

    def get_kafka_config(self):
        return self._kafka_config

    def get_kafka_producer_config(self):
        return self._kafka_producer_config

    # ========== 连接管理 ==========
    def connect(self) -> Set[str]:
        connected_set: set = set()
        if self._mysql_conn or self._connect_mysql():
            connected_set.add('mysql')
        if self._redis_client or self._connect_redis():
            connected_set.add('redis')
        if (self._kafka_admin and self._kafka_producer) or self._connect_kafka():
            connected_set.add('kafka')
        return connected_set

    def _connect_mysql(self) -> bool:
        """连接MySQL数据库"""
        try:
            self._mysql_conn = mysql.connector.connect(**self._mysql_config)
            return True
        except Error as e:
            print(f"MySQL连接失败: {e}")
            return False

    def _connect_redis(self) -> bool:
        """连接Redis数据库"""
        try:
            self._redis_client = Redis(**self._redis_config)
            # 测试连接
            self._redis_client.ping()
            return True
        except Exception as e:
            print(f"Redis连接失败: {e}")
            return False

    def _connect_kafka(self) -> bool:
        """连接Kafka"""
        try:
            self._kafka_producer = KafkaProducer(**self._kafka_producer_config)
            self._kafka_admin = KafkaAdminClient(
                bootstrap_servers=self._kafka_config.get('bootstrap_servers'),
                client_id=self._kafka_config.get('client_id', 'generic-db-handler')
            )
            return True
        except Exception as e:
            print(f"Kafka连接失败: {e}")
            return False

    def close(self) -> None:
        """关闭所有数据库连接"""
        if self._mysql_conn and self._mysql_conn.is_connected():
            self._mysql_conn.close()
        if self._redis_client:
            self._redis_client.close()
        if self._kafka_producer:
            self._kafka_producer.close()
        if self._kafka_admin:
            self._kafka_admin.close()
        # 关闭所有消费者
        for consumer_id, consumer in self._kafka_consumers.items():
            if consumer_id in self._consume_flags and self._consume_flags[consumer_id]:
                self.kafka_stop_continuous_consume(consumer_id)
            consumer.close()
        self._kafka_consumers.clear()

    # ========== MySQL 操作 ==========
    def mysql_clear_table(self, targets: Union[str, List[str]]) -> bool:
        try:
            if not self._mysql_conn:
                print("MySQL未连接")
                return False
            if isinstance(targets, str):
                targets = [targets]
            if isinstance(targets, list):
                for target in targets:
                    self._mysql_conn.cursor().execute(
                        f"DELETE FROM {target};"
                    )
                self._mysql_conn.commit()
                return True
            else:
                print(f"清空表失败: {type(targets)} is invalid.")
                return False
        except Exception as e:
            print(f"清空表失败: {e}")
            return False

    def mysql_create_table(self, table_name: str, schema: Dict[str, str], primary_key: str = None) -> bool:
        """
        创建MySQL表

        :param table_name: 表名
        :param schema: 表结构字典，如 {'id': 'INT', 'name': 'VARCHAR(50)'}
        :param primary_key: 主键字段名
        :return: 是否成功
        """
        if not self._mysql_conn:
            print("MySQL未连接")
            return False

        columns = [f"{name} {type_}" for name, type_ in schema.items()]
        if primary_key:
            columns.append(f"PRIMARY KEY ({primary_key})")

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"

        try:
            cursor = self._mysql_conn.cursor()
            cursor.execute(query)
            self._mysql_conn.commit()
            return True
        except Error as e:
            print(f"创建表失败: {e}")
            return False

    def mysql_insert(self, table_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """
        向MySQL插入数据

        :param table_name: 表名
        :param data: 单条数据(字典)或多条数据(字典列表)
        :return: 插入的行数
        """
        if not self._mysql_conn:
            print("MySQL未连接")
            return 0

        if isinstance(data, dict):
            data = [data]

        if not data:
            return 0

        columns = list(data[0].keys())
        placeholders = ["%s"] * len(columns)
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        try:
            cursor = self._mysql_conn.cursor()
            # 处理JSON类型数据
            prepared_data = []
            for item in data:
                prepared_item = []
                for col in columns:
                    if isinstance(item[col], (dict, list)):
                        prepared_item.append(json.dumps(item[col]))
                    else:
                        prepared_item.append(item[col])
                prepared_data.append(tuple(prepared_item))

            cursor.executemany(query, prepared_data)
            self._mysql_conn.commit()
            return cursor.rowcount
        except Error as e:
            print(f"插入数据失败: {e}")
            return 0

    def mysql_select(self, table_name: str,
                     columns: List[str] = None,
                     where: Dict[str, Any] = None,
                     limit: int = None) -> List[Dict[str, Any]]:
        """
        从MySQL查询数据

        :param table_name: 表名
        :param columns: 要查询的列名列表，None表示所有列
        :param where: 条件字典，如 {'id': 1, 'name': 'John'}
        :param limit: 限制返回的行数
        :return: 查询结果列表
        """
        if not self._mysql_conn:
            print("MySQL未连接")
            return []

        select_cols = "*" if not columns else ", ".join(columns)
        query = f"SELECT {select_cols} FROM {table_name}"
        params = []

        if where:
            conditions = []
            for k, v in where.items():
                conditions.append(f"{k} = %s")
                params.append(v)
            query += " WHERE " + " AND ".join(conditions)

        if limit:
            query += f" LIMIT {limit}"

        try:
            cursor = self._mysql_conn.cursor(dictionary=True)
            cursor.execute(query, tuple(params))
            return cursor.fetchall()
        except Error as e:
            print(f"查询数据失败: {e}")
            return []

    def mysql_update(self, table_name: str,
                     data: Dict[str, Any],
                     where: Dict[str, Any]) -> int:
        """
        更新MySQL数据

        :param table_name: 表名
        :param data: 要更新的数据字典
        :param where: 条件字典
        :return: 影响的行数
        """
        if not self._mysql_conn:
            print("MySQL未连接")
            return 0

        if not data or not where:
            return 0

        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

        # 处理JSON类型数据
        set_values = []
        for v in data.values():
            if isinstance(v, (dict, list)):
                set_values.append(json.dumps(v))
            else:
                set_values.append(v)

        params = tuple(set_values + list(where.values()))

        try:
            cursor = self._mysql_conn.cursor()
            cursor.execute(query, params)
            self._mysql_conn.commit()
            return cursor.rowcount
        except Error as e:
            print(f"更新数据失败: {e}")
            return 0

    def mysql_delete(self, table_name: str, where: Dict[str, Any]) -> int:
        """
        从MySQL删除数据

        :param table_name: 表名
        :param where: 条件字典
        :return: 影响的行数
        """
        if not self._mysql_conn:
            print("MySQL未连接")
            return 0

        if not where:
            print("必须提供where条件以防止全表删除")
            return 0

        where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"

        try:
            cursor = self._mysql_conn.cursor()
            cursor.execute(query, tuple(where.values()))
            self._mysql_conn.commit()
            return cursor.rowcount
        except Error as e:
            print(f"删除数据失败: {e}")
            return 0

    # ========== Redis 操作 ==========
    def redis_set(self, key: str, value: Any, expire: int = 60 * 10) -> bool:
        """
        设置Redis键值

        :param key: 键名
        :param value: 值(自动JSON序列化)
        :param expire: 过期时间(秒) 600s
        :return: 是否成功
        """
        if not self._redis_client:
            print("Redis未连接")
            return False

        try:
            serialized = json.dumps(value)
            if expire:
                self._redis_client.setex(key, expire, serialized)
            else:
                self._redis_client.set(key, serialized)
            return True
        except Exception as e:
            print(f"Redis设置失败: {e}")
            return False

    async def redis_get(self, key: str) -> Any:
        """
        获取Redis键值

        :param key: 键名
        :return: 反序列化的值，None表示键不存在或出错
        """
        if not self._redis_client:
            print("Redis未连接")
            return None

        try:
            value = await self._redis_client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            print(f"Redis获取失败: {e}")
            return None

    def redis_keys(self, pattern: str) -> Any:
        """
        获取Redis键

        :param pattern: 键匹配正则表达式模式
        :return: 正则表达式匹配 keys 的所有键
        """
        if not self._redis_client:
            print("Redis未连接")
            return None
        try:
            values = self._redis_client.keys(pattern)
            return values if values else None
        except Exception as e:
            print(f"Redis获取失败: {e}")
            return None

    def redis_exists(self, *keys: str) -> bool:
        return self._redis_client.exists(*keys)

    def redis_hset(self, hash_key: str, mapping: Dict[str, Any]) -> bool:
        """
        设置Redis哈希表

        :param hash_key: 哈希表键名
        :param mapping: 字段-值映射(值会自动JSON序列化)
        :return: 是否成功
        """
        if not self._redis_client:
            print("Redis未连接")
            return False

        try:
            serialized = {k: json.dumps(v) for k, v in mapping.items()}
            self._redis_client.hset(hash_key, mapping=serialized)
            return True
        except Exception as e:
            print(f"Redis哈希设置失败: {e}")
            return False

    def redis_hget(self, hash_key: str, field: str) -> Any:
        """
        获取Redis哈希表字段值

        :param hash_key: 哈希表键名
        :param field: 字段名
        :return: 反序列化的值
        """
        if not self._redis_client:
            print("Redis未连接")
            return None

        try:
            value = self._redis_client.hget(hash_key, field)
            return json.loads(value) if value else None
        except Exception as e:
            print(f"Redis哈希获取失败: {e}")
            return None

    def redis_hgetall(self, hash_key: str) -> Dict[str, Any]:
        """
        获取Redis哈希表所有字段

        :param hash_key: 哈希表键名
        :return: 字段-值字典(值已反序列化)
        """
        if not self._redis_client:
            print("Redis未连接")
            return {}

        try:
            data = self._redis_client.hgetall(hash_key)
            return {k.decode() if hasattr(k, 'decode') else str(k): json.loads(v) if isinstance(v, str) else v for k, v
                    in data.items()}
        except Exception as e:
            print(f"Redis哈希获取失败: {e}")
            return {}

    def redis_delete(self, *keys: str) -> int:
        """
        删除Redis键

        :param keys: 要删除的键名
        :return: 删除的键数量
        """
        if not self._redis_client:
            print("Redis未连接")
            return 0

        try:
            return self._redis_client.delete(*keys)
        except Exception as e:
            print(f"Redis删除失败: {e}")
            return 0

    def redis_clear(self) -> bool:
        if not self._redis_client:
            print("Redis未连接")
            return False

        try:
            self._redis_client.flushdb()
            return True
        except Exception as e:
            print(f"清空Redis数据时出错: {e}")
            return False

    # 3. Set 操作（新增，原生支持）
    def redis_sadd(self, set_key, *members):
        """
        往 Set 里添加元素
        :param set_key: Set 的键
        :param members: 要添加的元素（可变参数）
        :return: 添加成功的数量
        """
        if not self._redis_client:
            print("Redis未连接")
            return False
        return self._redis_client.sadd(set_key, *members)

    def redis_smembers(self, set_key):
        """
        获取 Set 所有元素
        """
        if not self._redis_client:
            print("Redis未连接")
            return False
        return self._redis_client.smembers(set_key)

    def redis_sinter(self, *set_keys):
        """
        多个 Set 取交集
        """
        if not self._redis_client:
            print("Redis未连接")
            return False
        return self._redis_client.sinter(*set_keys)

    # 4. ZSet（有序集合）操作（新增，原生支持）
    def redis_zadd(self, zset_key, *args, **kwargs):
        """
        往 ZSet 里添加元素（支持 score - member 对）
        用法：redis_zadd("my_zset", 10, "member1", 20, "member2")
             或 redis_zadd("my_zset", member1=10, member2=20)
        """
        if not self._redis_client:
            print("Redis未连接")
            return False
        return self._redis_client.zadd(zset_key, *args, **kwargs)

    def redis_zrangebyscore(self, zset_key, min_score, max_score):
        """
        按分数范围取 ZSet 元素
        """
        if not self._redis_client:
            print("Redis未连接")
            return False
        return self._redis_client.zrangebyscore(zset_key, min_score, max_score)

    # ========== Kafka 操作 ==========
    def kafka_exist_topic(self, topic_name: str) -> bool:
        return topic_name in self._kafka_admin.list_topics()

    def kafka_create_topic(self, topic_name: str,
                           num_partitions: int = 1,
                           replication_factor: int = 1,
                           config: Dict[str, str] = None) -> bool:
        """
        创建Kafka主题

        :param topic_name: 主题名
        :param num_partitions: 分区数
        :param replication_factor: 副本因子
        :param config: 主题配置
        :return: 是否成功
        """
        if not self._kafka_admin:
            print("Kafka未连接")
            return False
        if topic_name in self._kafka_admin.list_topics():
            print(f"主题已存在: {topic_name}")
            return False
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=config or {}
            )
            self._kafka_admin.create_topics([topic])
            return True
        except TopicAlreadyExistsError:
            print(f"主题已存在: {topic_name}")
            return True
        except Exception as e:
            print(f"创建主题失败: {e}")
            return False

    def kafka_produce(self, topic: str,
                      value: Any,
                      key: Optional[str] = None) -> bool:
        """
        向Kafka发送消息

        :param topic: 主题名
        :param value: 消息值(自动JSON序列化)
        :param key: 消息键(可选)
        :return: 是否成功
        """
        if not self._kafka_producer:
            print("Kafka未连接")
            return False
        if topic not in self._kafka_admin.list_topics():
            print(f"此主题不存在，请先创建此主题: '{topic}'")
            return False
        try:
            if key:
                self._kafka_producer.send(topic, key=key.encode(), value=value)
            else:
                self._kafka_producer.send(topic, value=value)
            return True
        except Exception as e:
            print(f"发送消息失败: {e}")
            return False

    def kafka_produce_batch(self, topic: str,
                            messages: List[Dict[str, Any]]) -> int:
        """
        批量向Kafka发送消息

        :param topic: 主题名
        :param messages: 消息列表，每个元素是包含value和可选key的字典
        :return: 成功发送的消息数
        """
        if not self._kafka_producer:
            print("Kafka未连接")
            return 0

        success = 0
        for msg in messages:
            try:
                key = msg.get('key')
                value = msg['value']
                if key:
                    self._kafka_producer.send(topic, key=key.encode(), value=value)
                else:
                    self._kafka_producer.send(topic, value=value)
                success += 1
            except Exception as e:
                print(f"发送消息失败: {e}")

        try:
            self._kafka_producer.flush()
        except Exception as e:
            print(f"刷新生产者失败: {e}")

        return success

    def kafka_delete_topic(self, topic_names: Union[str, List[str]]) -> bool:
        """
        删除Kafka主题

        :param topic_names: 要删除的主题名
        :return: 是否成功
        """
        if not self._kafka_admin:
            print("Kafka未连接")
            return False

        if isinstance(topic_names, str):
            topic_names = [topic_names]

        exist_topics = self._kafka_admin.list_topics()
        for topic in topic_names:
            if topic not in exist_topics:
                print(f"不存在此话题 '{topic}'，已取消删除")
                topic_names.remove(topic)

        try:
            # 注意: 需要设置delete.topic.enable=true在broker配置中才能删除主题
            self._kafka_admin.delete_topics(topic_names)
            if len(topic_names) > 0:
                print(f"主题 {', '.join([f'<{topic}>' for topic in topic_names])} 删除请求已发送")
                time.sleep(1)
            return True
        except Exception as e:
            print(f"删除主题失败: {e}")
            return False

    # ========== Kafka 消费者操作 ==========
    _kafka_static_group_id: int = 0

    def _kafka_random_group_id(self) -> str:
        self._kafka_static_group_id += 1
        return f"consumer-group-{self._kafka_static_group_id}"

    def kafka_create_consumer(self,
                              consumer_id: str,
                              topics: List[str],
                              group_id: str = None,
                              auto_offset_reset: str = 'earliest',
                              enable_auto_commit: bool = False,
                              **kwargs) -> Union[None, KafkaConsumer]:
        """
        创建Kafka消费者

        :param consumer_id: 消费者标识(用于管理多个消费者)
        :param topics: 订阅的主题列表
        :param group_id: 消费者组ID
        :param auto_offset_reset: 当没有初始偏移量时从哪里开始('earliest' 或 'latest')
        :param enable_auto_commit: 是否自动提交偏移量
        :param kwargs: 其他Kafka消费者配置
        :return: 创建的消费者实例
        """
        if not self._kafka_config:
            print("Kafka未配置")
            return None
        if consumer_id in self._kafka_consumers:
            print("此ID消费者已存在，已取消重复创建")
            return self._kafka_consumers[consumer_id]
        group_id = group_id or self._kafka_random_group_id()
        try:
            config = {
                'bootstrap_servers': self._kafka_config.get('bootstrap_servers', 'localhost:9092'),
                'group_id': group_id,
                'auto_offset_reset': auto_offset_reset,
                'enable_auto_commit': enable_auto_commit,
                'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
                'key_deserializer': lambda x: x.decode('utf-8') if x else None,
                **kwargs
            }
            consumer = KafkaConsumer(
                **config
            )
            consumer.subscribe(topics)
            self._kafka_consumers[consumer_id] = consumer
            return consumer
        except Exception as e:
            print(f"创建消费者失败: {e}")
            return None

    def kafka_get_consumer(self, consumer_id: str) -> Union[None, KafkaConsumer]:
        return self._kafka_consumers[consumer_id] if consumer_id in self._kafka_consumers else None

    def kafka_get_flink_consumer(self, consumer_id: str,
                                 deserialization_schema: DeserializationSchema = SimpleStringSchema()) \
            -> Union[None, FlinkKafkaConsumer]:
        if consumer_id not in self._kafka_consumers:
            print(f"不存在此消费者实例: '{consumer_id}'")
            return None
        consumer = self._kafka_consumers[consumer_id]
        return FlinkKafkaConsumer(
            topics=list(consumer.subscription()),
            deserialization_schema=deserialization_schema,
            properties={
                'bootstrap.servers': consumer.config['bootstrap_servers'],
                'group.id': consumer.config['group_id'],
                'auto.offset.reset': consumer.config['auto_offset_reset']
            }
        )

    def kafka_consume(self,
                      consumer_id: str,
                      timeout_ms: int = 1000,
                      max_records: int = None,
                      callback: Callable[[Any], None] = None) -> List[Any]:
        """
        从Kafka消费消息

        :param consumer_id: 消费者标识
        :param timeout_ms: 超时时间(毫秒)
        :param max_records: 最大记录数
        :param callback: 每条消息的回调函数
        :return: 消息列表
        """
        if consumer_id not in self._kafka_consumers:
            print(f"消费者 {consumer_id} 不存在")
            return []

        consumer = self._kafka_consumers[consumer_id]
        messages = []

        try:
            records = consumer.poll(
                timeout_ms=timeout_ms,
                max_records=max_records
            )

            for tp, msgs in records.items():
                for msg in msgs:
                    message = {
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'key': msg.key,
                        'value': msg.value,
                        'headers': {k: v.decode() for k, v in (msg.headers or [])},
                        'timestamp': msg.timestamp
                    }
                    messages.append(message)
                    if callback:
                        callback(message)

            return messages
        except Exception as e:
            print(f"消费消息失败: {e}")
            return []

    def kafka_start_continuous_consume(self,
                                       consumer_id: str,
                                       callback: Callable[[Any], None] = None,
                                       end_callback: Callable[[], None] = None,
                                       timeout_ms: int = 1000,
                                       daemon: bool = True,
                                       in_debug_mode: bool = True) -> None:
        """
        开始持续消费消息

        :param consumer_id: 消费者标识
        :param callback: 每条消息的回调函数
        :param end_callback: 消费结束时的回调函数
        :param timeout_ms: 每次poll的超时时间
        :param daemon: 是否以守护线程运行
        :param in_debug_mode: 是否以 debug 模式运行
        """
        if consumer_id not in self._kafka_consumers:
            print(f"消费者 {consumer_id} 不存在")
            return
        self._consume_flags[consumer_id] = True

        def consume_loop(ecb):
            times = 0
            while self._consume_flags.get(consumer_id, False):
                times += 1
                if in_debug_mode:
                    print(f"消费者 [{consumer_id}] 开始第 {times} 次消费")
                self.kafka_consume(
                    consumer_id=consumer_id,
                    timeout_ms=timeout_ms,
                    callback=callback
                )
            print(f"消费者 [{consumer_id}] 已停止消费")
            if ecb:
                ecb()

        consume_thread = threading.Thread(
            target=consume_loop,
            args=[end_callback])
        consume_thread.daemon = daemon
        consume_thread.start()
        print(f"消费者 [{consumer_id}] 已开始持续消费")

    def kafka_stop_continuous_consume(self, consumer_id: str) -> None:
        """
        停止持续消费

        :param consumer_id: 消费者标识
        """
        if consumer_id in self._consume_flags:
            if self._consume_flags[consumer_id]:
                self._consume_flags[consumer_id] = False
                print(f"已发送停止指令给消费者 [{consumer_id}]")
        else:
            print(f"消费者 [{consumer_id}] 未在持续消费中")

    def kafka_commit_offsets(self,
                             consumer_id: str,
                             offsets: Dict[TopicPartition, OffsetAndMetadata] = None) -> bool:
        """
        提交偏移量

        :param consumer_id: 消费者标识
        :param offsets: 要提交的偏移量(可选，None表示提交当前偏移量)
        :return: 是否成功
        """
        if consumer_id not in self._kafka_consumers:
            print(f"消费者 {consumer_id} 不存在")
            return False

        consumer = self._kafka_consumers[consumer_id]

        try:
            if offsets:
                consumer.commit(offsets=offsets)
            else:
                consumer.commit()
            return True
        except Exception as e:
            print(f"提交偏移量失败: {e}")
            return False

    def kafka_seek_to_beginning(self,
                                consumer_id: str,
                                topic_partitions: List[TopicPartition] = None) -> bool:
        """
        将消费者定位到分区开头

        :param consumer_id: 消费者标识
        :param topic_partitions: 要定位的分区列表(可选，None表示所有分区)
        :return: 是否成功
        """
        if consumer_id not in self._kafka_consumers:
            print(f"消费者 {consumer_id} 不存在")
            return False

        consumer = self._kafka_consumers[consumer_id]

        try:
            consumer.seek_to_beginning(topic_partitions)
            return True
        except Exception as e:
            print(f"定位到分区开头失败: {e}")
            return False

    def kafka_get_consumer_offsets(self,
                                   consumer_id: str) -> Dict[TopicPartition, int]:
        """
        获取消费者当前偏移量

        :param consumer_id: 消费者标识
        :return: 分区到偏移量的映射
        """
        if consumer_id not in self._kafka_consumers:
            print(f"消费者 {consumer_id} 不存在")
            return {}

        consumer = self._kafka_consumers[consumer_id]

        try:
            return {tp: consumer.position(tp) for tp in consumer.assignment()}
        except Exception as e:
            print(f"获取偏移量失败: {e}")
            return {}

    def kafka_close_consumer(self, consumer_id: str) -> bool:
        """
        关闭Kafka消费者

        :param consumer_id: 消费者标识
        :return: 是否成功
        """
        if consumer_id not in self._kafka_consumers:
            print(f"消费者 {consumer_id} 不存在")
            return False

        try:
            consumer = self._kafka_consumers.pop(consumer_id)
            consumer.close()
            return True
        except Exception as e:
            print(f"关闭消费者失败: {e}")
            return False


def main():
    # 初始化
    manager = DataBaseManager(
        kafka_config={
            'bootstrap_servers': 'localhost:9092',
            'client_id': 'test-client'
        }
    )

    # 创建主题
    manager.kafka_create_topic('test_topic')

    # 生产消息
    manager.kafka_produce('test_topic', {'message': 'Hello Kafka'}, key='key1')

    # 创建消费者
    manager.kafka_create_consumer(
        consumer_id='test_consumer',
        group_id='test_group',
        topics=['test_topic']
    )

    manager.kafka_get_flink_consumer('test_consumer')

    # 消费消息
    def process_message(msg):
        print(f"收到消息: {msg}")

    manager.kafka_consume('test_consumer', callback=process_message)

    # 手动提交偏移量
    manager.kafka_commit_offsets('test_consumer')

    # 获取消费者偏移量
    offsets = manager.kafka_get_consumer_offsets('test_consumer')
    print(f"当前偏移量: {offsets}")

    manager.kafka_delete_topic(['test_topic', 'not-exist-topic'])

    # 关闭消费者
    manager.kafka_close_consumer('test_consumer')


if __name__ == '__main__':
    main()
