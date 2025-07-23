#
# Created by RestRegular on 2025/7/22
#
import threading
from typing import Dict, Any, List, Optional, Callable
import sys
import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import BasicTypeInfo

from src.components.database_manager import DataBaseManager
from src.components.flink_matching_processor import ResumeMatcher


class RJMatcher:
    """
    职位-简历匹配引擎核心类

    该类负责协调职位数据存储、简历数据传输以及基于Flink的实时职位-简历匹配流程。
    主要功能包括：
    - 将职位数据批量上传到Redis并建立多维度索引
    - 将简历数据批量发送到Kafka消息队列
    - 通过Flink流处理引擎实现职位与简历的实时匹配
    - 提供匹配结果的订阅功能

    依赖组件：
    - Redis：用于存储职位数据及建立查询索引
    - Kafka：用于简历数据的传输和匹配结果的输出
    - Flink：用于实现实时流处理和匹配逻辑

    典型使用流程：
    1. 初始化RJMatcher实例，配置Redis、Kafka和Flink连接信息
    2. 调用upload_job_datas()方法上传职位数据到Redis
    3. 调用upload_resume_datas()方法将简历数据发送到Kafka
    4. 调用match()方法启动Flink匹配任务
    5. 可选：调用subscribe_result()方法订阅匹配结果
    6. 完成后调用close()方法释放资源
    """

    def __init__(self,
                 redis_config: Dict[str, Any],
                 kafka_config: Dict[str, Any],
                 flink_config: Dict[str, Any]):
        """
        初始化RJMatcher实例

        参数:
            redis_config: Dict[str, Any]
                Redis数据库配置字典，包含连接所需的相关参数，如：
                - host: Redis服务器主机地址
                - port: Redis服务器端口号
                - db: 数据库编号
                - password: 连接密码（可选）
                - timeout: 连接超时时间（可选）

            kafka_config: Dict[str, Any]
                Kafka消息队列配置字典，包含：
                - bootstrap_servers: Kafka服务器地址列表，格式为"host:port,host:port"
                - source_topic: 简历数据输入主题名称
                - target_topic: 匹配结果输出主题名称
                - 其他Kafka相关配置参数

            flink_config: Dict[str, Any]
                Flink流处理引擎配置字典，包含：
                - parallelism: 并行度设置
                - lib_jar_path: Flink所需的外部JAR包路径，可为字符串或列表
        """
        self._db_manager = DataBaseManager(
            redis_config=redis_config,
            kafka_config=kafka_config)
        self._flink_config = flink_config
        self._kafka_producer_config = {
            'bootstrap.servers': kafka_config["bootstrap_servers"],
            'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
            'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer'
        }
        if self._db_manager.kafka_exist_topic(kafka_config['source_topic']):
            self._db_manager.kafka_delete_topic(kafka_config['source_topic'])
        if self._db_manager.kafka_exist_topic(kafka_config['target_topic']):
            self._db_manager.kafka_delete_topic(kafka_config['target_topic'])

    def set_kafka_producer_config(self, **configs):
        """
        更新 Kafka 生产者配置，合并新配置到已有配置中
        该方法会将传入的配置参数合并到已有的 Kafka 生产者配置中，
        若存在相同的配置项，新传入的配置将覆盖原有配置值。
        参数:
        ** configs: 关键字参数形式的 Kafka 生产者配置项
        """
        self._kafka_producer_config = {
            **self._kafka_producer_config,
            **configs
        }

    def upload_job_datas(self, jobs: List[Dict[str, Any]]):
        """
        批量上传职位数据到Redis，建立多维度索引以支持高效查询

        该方法将职位数据存储到Redis中，并针对不同查询维度建立索引，
        包括职位详情的哈希存储、分类/地区/技能的集合索引，以及薪资/经验的有序集合索引，
        便于后续根据多条件组合查询职位信息。

        参数:
            jobs: List[Dict[str, Any]]
                待上传的职位数据列表，每个职位为包含以下字段的字典：
                - job_id: str 或 int，职位唯一标识符（必需）
                - category: str，职位所属分类（如"技术开发"、"市场营销"等，必需）
                - location: str，职位所在地区（如"北京"、"上海"等，必需）
                - required_skills: List[str]，职位要求的技能列表（如["Python", "SQL"]等，必需）
                - salary_low: int 或 float，职位最低薪资（单位通常为元/月，必需）
                - required_experience: int 或 float，职位要求的最低工作经验（单位通常为年，必需）
                - 其他自定义字段: 任意类型，职位的其他详情信息（如职位名称、职责描述等，可选）

        存储逻辑:
            1. 职位详情存储： 使用Hash结构存储每个职位的完整字段，键格式为"job:info:{job_id}"
            2. 分类索引： 使用Set结构存储同一分类下的所有职位ID，键格式为"job:category:{category}"
            3. 地区索引： 使用Set结构存储同一地区下的所有职位ID，键格式为"job:location:{location}"
            4. 技能索引： 使用Set结构存储需要特定技能的所有职位ID，键格式为"job:skill:{skill}"
            5. 最低薪资索引： 使用ZSet结构存储所有职位ID及其对应的最低薪资，键为"job:salary:low"，可用于按薪资范围查询
            6. 工作经验索引： 使用ZSet结构存储所有职位ID及其对应的要求经验，键为"job:experience"，可用于按经验要求范围查询

        异常:
            RedisConnectionError: 当与Redis的连接失败时抛出
            KeyError: 当职位数据缺少必需字段时抛出
        """
        for job in jobs:
            job_id = job["job_id"]
            # 1. 用 Hash 存职位详情（字段级存储，方便后续按需读取）
            hash_key = f"job:info:{job_id}"
            for field, value in job.items():
                self._db_manager.redis_hset(hash_key, {field: value})

            # 2. 用 Set 建分类索引（category）
            category = job["category"]
            category_set_key = f"job:category:{category}"
            self._db_manager.redis_sadd(category_set_key, job_id)

            # 3. 用 Set 建地区索引（location）
            location = job["location"]
            location_set_key = f"job:location:{location}"
            self._db_manager.redis_sadd(location_set_key, job_id)

            # 4. 用 Set 建技能索引（required_skills）
            for skill in job["required_skills"]:
                skill_set_key = f"job:skill:{skill}"
                self._db_manager.redis_sadd(skill_set_key, job_id)

            # 5. 用 ZSet 建最低薪资索引（salary_low）
            salary_low = job["salary_low"]
            salary_low_zset_key = "job:salary:low"
            self._db_manager.redis_zadd(salary_low_zset_key, {job_id: salary_low})

            # 6. 用 ZSet 建工作经验索引（required_experience）
            experience = job["required_experience"]
            experience_zset_key = "job:experience"
            self._db_manager.redis_zadd(experience_zset_key, {job_id: experience})

    def upload_resume_datas(self, resumes: List[Dict[str, Any]]):
        """
        批量上传简历数据到Kafka指定主题

        该方法用于将简历数据批量发送到Kafka消息队列的指定主题中，
        并在主题不存在时自动创建。适用于简历数据的异步处理、分发或存储场景。

        参数:
            resumes: List[Dict[str, Any]]
                待上传的简历数据列表，每个元素为包含简历信息的字典，
                包含以下字段：
                - resume_id: str 或 int，简历唯一标识符
                - name: str，姓名
                - age: int，年龄
                - gender: str，性别（如"男"、"女"）
                - education: str，学历（如"本科"、"硕士"等）
                - experience: int 或 float，工作经验（通常以年为单位）
                - category: str，意向职位类别
                - skills: List[str]，技能列表
                - expected_salary: int 或 float，期望薪资
                - location: str，意向工作地点

        处理逻辑:
            1. 检查指定的Kafka主题是否存在
            2. 若主题不存在，则创建该Kafka主题
            3. 将简历列表批量打包为Kafka消息格式（以字典形式包含value字段）
            4. 通过Kafka生产者将批量消息发送到指定主题

        注意:
            - 简历数据字典的字段应与业务系统中定义的简历模型保持一致
            - 确保Kafka服务已正确配置并可连接，否则可能导致发送失败
            - 批量发送的消息大小应考虑Kafka的配置限制，过大的消息可能需要拆分处理

        异常:
            KafkaConnectionError: 当与Kafka的连接失败时抛出
            KeyError: 当简历数据缺少必需字段时抛出
            MessageTooLargeError: 当消息大小超过Kafka配置限制时抛出
        """
        source_topic = self._db_manager.get_kafka_config()["source_topic"]
        if not self._db_manager.kafka_exist_topic(source_topic):
            self._db_manager.kafka_create_topic(source_topic)
        self._db_manager.kafka_produce_batch(
            topic=source_topic,
            messages=[{'value': r} for r in resumes])

    def _match(self, in_debug_mode: bool = True):
        """
        启动Flink流处理任务，实现职位与简历的实时匹配

        该方法创建并配置Flink流处理环境，从Kafka读取简历数据，
        根据职位类别进行分区后，使用ResumeMatcher进行匹配计算，
        最后将匹配结果输出到Kafka目标主题。支持调试模式以便于开发测试。

        参数:
            in_debug_mode: bool, 可选
                是否启用调试模式，默认为True。
                调试模式下会将匹配结果打印到控制台，便于开发调试。

        处理流程:
            1. 初始化Flink执行环境并设置并行度
            2. 添加Flink所需的外部JAR包依赖
            3. 创建Kafka消费者，从源主题读取简历数据
            4. 将简历数据流按职位类别进行分区
            5. 对每个分区应用ResumeMatcher进行职位-简历匹配
            6. 若启用调试模式，将结果打印到控制台
            7. 创建Kafka生产者，将匹配结果写入目标主题
            8. 执行Flink任务，等待任务完成或中断

        注意:
            - 确保Flink集群已正确配置并可访问
            - ResumeMatcher类需实现具体的匹配逻辑
            - 任务可通过KeyboardInterrupt（Ctrl+C）中断

        异常:
            FlinkConfigurationError: 当Flink配置错误时抛出
            KafkaConsumerError: 当Kafka消费者创建失败时抛出
            JobExecutionException: 当Flink任务执行失败时抛出
        """
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(self._flink_config['parallelism'])
        lib_jars = self._flink_config['lib_jar_path']
        env.add_jars(*lib_jars if isinstance(lib_jars, list) else str(lib_jars))
        source_topic = self._db_manager.get_kafka_config()["source_topic"]
        self._db_manager.kafka_create_consumer(
            consumer_id="resume-data-source",
            topics=[source_topic]
        )
        resumes_source = env.add_source(
            self._db_manager.kafka_get_flink_consumer('resume-data-source')
        )
        # 按职位类别分区，确保同类职位和简历在同一处理节点
        partitioned_resumes = resumes_source.key_by(lambda x: json.loads(x).get("category", ""))
        # 应用匹配逻辑
        resume_matches = partitioned_resumes.map(ResumeMatcher(self._db_manager.get_redis_config()))
        if in_debug_mode:
            resume_matches.print("Debug Print")

        resume_matches = resume_matches.flat_map(
            lambda results: [json.dumps({
                'resume_id': result[0],
                'job_id': result[1],
                'score': result[2]
            }) for result in results],
            output_type=BasicTypeInfo.STRING_TYPE_INFO())

        # 配置结果输出到Kafka ToDo: This part needs to be tested.
        target_topic = self._db_manager.get_kafka_config()["target_topic"]
        if not self._db_manager.kafka_exist_topic(target_topic):
            self._db_manager.kafka_create_topic(target_topic)
        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers(self._kafka_producer_config['bootstrap.servers']) \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(target_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ).build()
        resume_matches.sink_to(kafka_sink)
        try:
            env.execute("Job-Resume Matching")
        except KeyboardInterrupt:
            print(f"Stop executing job 'Job-Resume Matching'.")
            sys.exit(0)

    def start_match(self, in_debug_mode: bool = True) -> threading.Thread:
        """
        在新线程中启动match方法
        参数:
            in_debug_mode: 是否启用调试模式
        返回:
            已启动的线程对象
        """
        match_thread = threading.Thread(
            target=self._match,
            args=(in_debug_mode,),
            name="Resume-Match-Thread"
        )
        match_thread.daemon = True
        match_thread.start()
        return match_thread

    def subscribe_result(self,
                         time_out: Optional[int] = 10000,
                         max_message_size: Optional[int] = None,
                         single_message_callback: Callable[[Any], None] = None,
                         all_messages_callback: Callable[[List[Any]], None] = None):
        """
        订阅并消费职位-简历匹配结果

        该方法创建Kafka消费者，从匹配结果输出主题中消费消息，
        支持设置最大消息数量和自定义回调函数，适用于实时获取匹配结果。

        参数:
            time_out: Optional[int], 可选
                超时时间(毫秒), 默认为10000毫秒

            max_message_size: Optional[int], 可选
                最大消费消息数量，默认为None（持续消费直到被中断）。
                若指定该参数，当消费消息数量达到该值时将停止消费。

            callback: Callable[[str], None], 可选
                消息处理回调函数，默认为None。
                若提供该函数，每条消息都会调用该函数进行处理；
                若未提供，则将消息收集到列表中并在结束时返回。

        注意:
            - 消费者配置为从最新偏移量开始消费（auto_offset_reset='latest'）
            - 启用自动提交偏移量，提交间隔为5000ms
            - 可通过KeyboardInterrupt（Ctrl+C）中断消费过程

        异常:
            KafkaConsumerError: 当Kafka消费者创建失败时抛出
            ConsumptionError: 当消息消费过程中发生错误时抛出
        """
        # ToDo: This part needs to be tested.
        self._db_manager.kafka_create_consumer(
            "consume-result",
            topics=[self._db_manager.get_kafka_config()['target_topic']],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            key_deserializer=lambda key: str(key)
        )
        result_count = 0

        def cb(result):
            nonlocal result_count
            result_count += 1
            if max_message_size and result_count >= max_message_size:
                self._db_manager.kafka_stop_continuous_consume("consume-result")
            else:
                single_message_callback(result)
        self._db_manager.kafka_start_continuous_consume(
            "consume-result",
            timeout_ms=time_out,
            callback=cb,
            end_callback=all_messages_callback
        )

    def cancel_subscribe_result(self):
        """
        此方法用于在未指定最大消费消息数量的情况下手动停止订阅消费匹配结果
        """
        self._db_manager.kafka_stop_continuous_consume("consume-result")

    def close(self) -> None:
        """
        释放资源，关闭数据库连接

        该方法调用内部db_manager的close()方法，
        关闭所有与Redis和Kafka的连接，释放相关资源。

        建议在使用完RJMatcher实例后调用该方法，
        特别是在长时间运行的应用程序中，以避免资源泄漏。
        """
        self._db_manager.close()


def main():
    # 测试代码, 使用示例
    matcher = RJMatcher(
        redis_config={
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'decode_responses': True
        },
        kafka_config={
            'bootstrap_servers': 'localhost:9092',
            'client_id': 'job-matching-client',
            'source_topic': 'source-topic',
            'target_topic': 'target-topic'
        },
        flink_config={
            'parallelism': 5,
            'lib_jar_path': ["file:///usr/local/flink/lib/flink-connector-jdbc-1.17.2.jar",
                             "file:///usr/local/flink/lib/mysql-connector-j-8.0.33.jar"]
        })
    matcher.set_kafka_producer_config(
        batch_size=16384,
        buffer_memory=33554432)
    # Note: 测试使用模拟数据
    from src.components.data_generator import DataGenerator
    resumes, jobs = DataGenerator().generate_data(100, 10)
    matcher.upload_job_datas(jobs)
    matcher.upload_resume_datas(resumes)
    match_thread = matcher.start_match(True)
    results = []

    def callback(result):
        nonlocal results
        # Note: 可以在这里进一步处理每一个匹配结果
        results.append(result['value'])  # Note: 需要自己收集所有结果
        print(result['value'])

    def print_all_results():
        nonlocal results
        print(results)
    matcher.subscribe_result(time_out=1000, max_message_size=None,
                             single_message_callback=callback,
                             all_messages_callback=print_all_results)

    # Note: 需要将Flink计算线程加入到主线程中
    match_thread.join(20)
    # Note: 注意释放资源
    matcher.close()


if __name__ == '__main__':
    main()
