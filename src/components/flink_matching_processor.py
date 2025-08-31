#
# Created by RestRegular on 2025/7/18
#
import sys
import json
from typing import List, Tuple, Optional, Dict, Union, Any

from pyflink.common import Types, Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

from src.components.error_manager import RJMError
from src.components.error_manager import deprecated
from src.components.database_manager import DataBaseManager
from src.components.data_manager import ResumeDataBuilder, JobDataBuilder
from src.components.match_score_calculator import RJMatchDegreeCalculator, MatchDimensionWeightConfigBuilder

__all__ = [
    'set_match_threshold',
    'ResumeMatcher'
]

# 匹配阈值：分数大于等于此阈值的简历将被筛选出来
MATCH_THRESHOLD = 10


@deprecated("此函数已被弃用，请使用 src.components.match_score_calculator 中的 "
            "RJMatchDegreeCalculator.calculate_overall_match 方法计算更精确的匹配度")
def calculate_match_score(resume: dict, job: dict) -> float:
    """
    粗略计算简历与岗位的匹配度

    该方法通过四个维度计算匹配度并加权求和：
    - 技能匹配度（权重0.4）
    - 经验匹配度（权重0.3）
    - 薪资匹配度（权重0.2）
    - 类别匹配度（权重0.1）

    参数:
        resume: 简历数据字典，包含skills, experience, expected_salary, category等字段
        job: 岗位数据字典，包含required_skills, required_experience, salary_low等字段

    返回:
        float: 0-1之间的匹配度分数
    """
    # 技能匹配度计算
    resume_skills = set(resume.get("skills", []))
    job_skills = set(job.get("required_skills", []))

    if not job_skills:
        skill_match_score = 1.0
    else:
        matched_skills = resume_skills.intersection(job_skills)
        skill_match_score = len(matched_skills) / len(job_skills)

    # 经验匹配度计算
    resume_experience = resume.get("experience", 0)
    job_experience = job.get("required_experience", 0)

    if job_experience == 0:  # 避免除以0
        experience_match_score = 1.0
    elif resume_experience >= job_experience:
        experience_match_score = 1.0
    else:
        experience_match_score = resume_experience / job_experience

    # 薪资匹配度计算
    resume_salary = resume.get("expected_salary", 0)
    job_salary_low = job.get("salary_low", 0)
    job_salary_high = job.get("salary_high", 0)

    if job_salary_low == 0 and job_salary_high == 0:
        salary_match_score = 1.0
    elif job_salary_low <= resume_salary <= job_salary_high:
        salary_match_score = 1.0
    elif resume_salary < job_salary_low:
        salary_match_score = resume_salary / job_salary_low if job_salary_low != 0 else 0.0
    else:
        salary_match_score = job_salary_high / resume_salary if resume_salary != 0 else 0.0

    # 类别匹配度计算
    resume_category = resume.get("category", "")
    job_category = job.get("category", "")
    category_match_score = 1.0 if resume_category == job_category else 0.0

    # 综合得分计算
    return (0.4 * skill_match_score +
            0.3 * experience_match_score +
            0.2 * salary_match_score +
            0.1 * category_match_score)


def set_match_threshold(threshold: int):
    """
    设置简历与岗位匹配的阈值

    参数:
        threshold: 新的匹配阈值，整数类型
    """
    global MATCH_THRESHOLD
    MATCH_THRESHOLD = threshold


class ResumeMatcher(MapFunction):
    """
    处理简历流，计算与所有岗位的匹配度

    该类实现了PyFlink的MapFunction接口，用于在流处理中
    对每一份简历计算与所有岗位的匹配度，并返回符合阈值要求的匹配结果
    """

    def __init__(self, redis_config):
        """
        初始化ResumeMatcher

        参数:
            redis_config: Redis数据库配置字典，包含连接所需的host, port等信息
        """
        self.redis_config = redis_config
        self.redis_manager: Optional[DataBaseManager] = None

    def open(self, runtime_context):
        """
        初始化资源（在并行实例启动时调用）

        该方法会初始化Redis连接管理器，并设置匹配度计算器的权重配置
        """
        self.redis_manager = DataBaseManager(redis_config=self.redis_config)
        RJMatchDegreeCalculator.set_wight_config(
            MatchDimensionWeightConfigBuilder.with_default_config())

    def map(self, value: str) -> List[Tuple[int, int, float, Dict[str, Union[int, float]]]]:
        """
        处理单条简历数据，计算与所有岗位的匹配度

        参数:
            value: 序列化的简历JSON字符串

        返回:
            符合匹配阈值的结果列表，每个元素为元组(resume_id, job_id, 匹配度, 维度得分字典)
        """
        try:
            resume = json.loads(value)
            resume_id = resume.get("resume_id")
            if not resume_id:
                return []
            # 从Redis获取所有岗位的键
            job_keys = self.redis_manager.redis_keys("job:info:*")
            matches: List[Tuple[int, int, float, Dict[str, Union[int, float]]]] = []
            for job_key in job_keys:
                # 解析岗位ID并获取岗位数据
                job_id = int(job_key.split(":")[-1])
                job_data = self.redis_manager.redis_hgetall(job_key)
                # 计算匹配度并过滤
                degree, dimension_scores = RJMatchDegreeCalculator().calculate_overall_match(
                    resume_builder=ResumeDataBuilder.from_dict(resume),
                    job_builder=JobDataBuilder.from_dict(job_data)
                )
                if degree >= MATCH_THRESHOLD:
                    matches.append((resume_id, job_id, degree, dimension_scores))
            return matches
        except Exception as e:
            print(f"计算简历匹配度时出错: {str(e)}")
            return []


class JobMatcher(MapFunction):
    """
    处理简历流，计算与指定岗位列表的匹配度

    该类实现了PyFlink的MapFunction接口，用于在流处理中
    对每一份简历计算与指定岗位列表的匹配度，并返回符合阈值要求的匹配结果
    """

    def __init__(self, redis_config, job_ids: List[int]):
        """
        初始化JobMatcher

        参数:
            redis_config: Redis数据库配置字典
            job_ids: 需要匹配的岗位ID列表
        """
        self.redis_config = redis_config
        self.redis_manager: Optional[DataBaseManager] = None
        self.job_ids: List[int] = job_ids
        self.jobs: Dict[int, Dict[str, Any]] = {}

    def open(self, runtime_context):
        """
        初始化资源（在并行实例启动时调用）

        该方法会初始化Redis连接管理器，预加载指定的岗位数据，
        并设置匹配度计算器的权重配置
        """
        self.redis_manager = DataBaseManager(redis_config=self.redis_config)
        RJMatchDegreeCalculator.set_wight_config(
            MatchDimensionWeightConfigBuilder.with_default_config())
        for job_id in self.job_ids:
            key = f"job:info:{job_id}"
            if not self.redis_manager.redis_exists(key):
                raise RJMError("JobMatcher 待获取的 job Redis key 不存在错误",
                               [f"此岗位ID不存在: {job_id}"])
            self.jobs[job_id] = self.redis_manager.redis_hgetall(key)

    def map(self, value: str) -> List[Tuple[int, int, float, Dict[str, Union[int, float]]]]:
        """
        处理单条简历数据，计算与指定岗位的匹配度

        参数:
            value: 序列化的简历JSON字符串

        返回:
            符合匹配阈值的结果列表，每个元素为元组(resume_id, job_id, 匹配度, 维度得分字典)
        """
        try:
            resume = json.loads(value)
            resume_id = resume.get("resume_id")
            if not resume_id:
                return []

            matches: List[Tuple[int, int, float, Dict[str, Union[int, float]]]] = []

            for job_id, job_data in self.jobs.items():
                # 计算匹配度并过滤
                degree, dimension_scores = RJMatchDegreeCalculator().calculate_overall_match(
                    resume_builder=ResumeDataBuilder.from_dict(resume),
                    job_builder=JobDataBuilder.from_dict(job_data)
                )
                if degree >= MATCH_THRESHOLD:
                    matches.append((resume_id, job_id, degree, dimension_scores))

            return matches
        except Exception as e:
            print(f"计算岗位匹配度时出错: {str(e)}")
            return []


def main():
    """
    程序主入口

    该函数负责初始化Flink执行环境、配置数据源和数据汇，
    构建简历与岗位匹配的处理流程，并启动执行
    """
    # 数据库和消息队列配置
    mysql_config = {
        'host': 'localhost',
        'username': 'root',
        'password': '322809',  # 使用环境变量处理密码
        'database': 'job_matching',
        'url': 'jdbc:mysql://localhost:3306/job_matching?useSSL=false&serverTimezone=UTC',
        'table_name': 'resume_job_matches',
        'charset': 'utf8mb4'
    }

    redis_config = {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'decode_responses': True
    }

    kafka_config = {
        'bootstrap_servers': 'localhost:9092',
        'client_id': 'job-matching-client'
    }

    kafka_producer_config = {
        'batch_size': 16384,
        'buffer_memory': 33554432
    }

    # 初始化数据库管理器
    db_manager = DataBaseManager(mysql_config={
        'host': mysql_config['host'],
        'user': mysql_config['username'],
        'password': mysql_config['password'],
        'database': mysql_config['database'],
        'charset': mysql_config['charset']
    }, kafka_config=kafka_config,
        kafka_producer_config=kafka_producer_config)

    # 清除历史匹配数据
    db_manager.mysql_clear_table(mysql_config['table_name'])

    # 1. 创建Flink执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(5)  # 设置并行度

    # 添加JDBC和MySQL连接依赖
    env.add_jars("file:///usr/local/flink/lib/flink-connector-jdbc-1.17.2.jar",
                 "file:///usr/local/flink/lib/mysql-connector-j-8.0.33.jar")

    # 创建Kafka消费者
    db_manager.kafka_create_consumer(
        consumer_id="resume-data-source",
        topics=["resumes_topic"]
    )

    # 2. 获取简历数据源
    resumes_source = env.add_source(
        db_manager.kafka_get_flink_consumer('resume-data-source')
    )

    # 3. 按简历类别分区（优化并行处理）
    partitioned_resumes = resumes_source.key_by(lambda x: json.loads(x).get("category", ""))

    # 4. 处理流数据：计算简历与岗位匹配度
    resume_matches = partitioned_resumes.map(ResumeMatcher(redis_config))

    # 5. 创建JDBC Sink，将结果写入MySQL
    jdbc_sink = JdbcSink.sink(
        f"INSERT INTO {mysql_config['table_name']} (resume_id, job_id, match_score) VALUES (?, ?, ?) "
        "ON DUPLICATE KEY UPDATE match_score = VALUES(match_score)",
        type_info=RowTypeInfo(
            [Types.STRING(), Types.STRING(), Types.DOUBLE()],
            ['resume_id', 'job_id', 'match_score']
        ),
        jdbc_connection_options=JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(mysql_config['url'])
        .with_driver_name('com.mysql.cj.jdbc.Driver')
        .with_user_name(mysql_config['username'])
        .with_password(mysql_config['password'])
        .build(),
        jdbc_execution_options=JdbcExecutionOptions.builder()
        .with_batch_size(10)  # 批处理大小
        .with_max_retries(3)  # 最大重试次数
        .with_batch_interval_ms(100)  # 批处理间隔(毫秒)
        .build()
    )

    # 转换匹配结果为Flink Row类型
    matches_stream = resume_matches.flat_map(
        lambda matches: [
            Row(resume_id=str(match[0]), job_id=str(match[1]), match_score=float(match[2]))
            for match in matches
        ],
        output_type=RowTypeInfo(
            [Types.STRING(), Types.STRING(), Types.DOUBLE()],
            ['resume_id', 'job_id', 'match_score']
        )
    )

    # 调试输出
    matches_stream.print("Debug before JDBC Sink")

    # 添加 JDBC Sink
    matches_stream.add_sink(jdbc_sink)

    print("start to execute job...")

    # 6. 执行Flink作业
    try:
        env.execute("Job-Resume Matching")
    except KeyboardInterrupt:
        print(f"Stop executing job 'Job-Resume Matching'.")
        sys.exit(0)


if __name__ == "__main__":
    main()
