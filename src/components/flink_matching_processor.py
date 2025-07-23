#
# Created by RestRegular on 2025/7/18
#
import sys
import json
from typing import List, Tuple, Optional

from pyflink.common import Types, Row
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions

from src.components.database_manager import DataBaseManager

# 定义匹配阈值, 分数大于等于阈值的简历将被筛选出来
MATCH_THRESHOLD = 0.5


def calculate_match_score(resume: dict, job: dict) -> float:
    """计算简历与岗位的匹配度"""
    # 技能匹配度
    resume_skills = set(resume.get("skills", []))
    job_skills = set(job.get("required_skills", []))

    if not job_skills:
        skill_match_score = 1.0
    else:
        matched_skills = resume_skills.intersection(job_skills)
        skill_match_score = len(matched_skills) / len(job_skills)

    # 经验匹配度
    resume_experience = resume.get("experience", 0)
    job_experience = job.get("required_experience", 0)

    if job_experience == 0:  # 避免除以0
        experience_match_score = 1.0
    elif resume_experience >= job_experience:
        experience_match_score = 1.0
    else:
        experience_match_score = resume_experience / job_experience

    # 薪资匹配度
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

    # 类别匹配度
    resume_category = resume.get("category", "")
    job_category = job.get("category", "")
    category_match_score = 1.0 if resume_category == job_category else 0.0

    # 综合得分: 为每种维度设定权重
    return (0.4 * skill_match_score +
            0.3 * experience_match_score +
            0.2 * salary_match_score +
            0.1 * category_match_score)


class ResumeMatcher(MapFunction):
    """处理简历流，计算与所有岗位的匹配度"""

    def __init__(self, redis_config):
        self.redis_config = redis_config
        self.redis_manager: Optional[DataBaseManager] = None

    def open(self, runtime_context):
        self.redis_manager = DataBaseManager(redis_config=self.redis_config)

    def map(self, value: str) -> List[Tuple[str, str, float]]:
        try:
            resume = json.loads(value)
            resume_id = resume.get("resume_id")
            if not resume_id:
                return []

            # 从Redis获取所有岗位的键
            job_keys = self.redis_manager.redis_gets("job:info:*")
            matches = []

            for job_key in job_keys:
                # 解析岗位ID并获取岗位数据
                job_id = job_key.split(":")[-1]
                job_data = self.redis_manager.redis_hgetall(job_key)
                job = {
                    "required_skills": job_data.get("required_skills", []),
                    "required_experience": int(job_data.get('required_experience', 0)),
                    "category": job_data.get("category"),
                    "salary_low": int(job_data.get("salary_low")),
                    "salary_high": int(job_data.get("salary_high"))
                }
                # 计算匹配度并过滤
                score = calculate_match_score(resume, job)
                if score >= MATCH_THRESHOLD:
                    matches.append((str(resume_id), str(job_id), round(score, 2)))

            return matches
        except Exception as e:
            print(f"处理简历出错: {str(e)}")
            return []


def main():
    # Mysql、Redis、Kafka相关组件的配置
    mysql_config = {
        'host': 'localhost',
        'username': 'root',
        'password': '322809',  # 可以使用环境变量处理密码
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

    db_manager = DataBaseManager(mysql_config={
        'host': mysql_config['host'],
        'user': mysql_config['username'],
        'password': mysql_config['password'],
        'database': mysql_config['database'],
        'charset': mysql_config['charset']
    }, kafka_config=kafka_config,
        kafka_producer_config=kafka_producer_config)

    # 先将历史数据清除
    db_manager.mysql_clear_table(mysql_config['table_name'])

    # 1. 创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(5)  # 增加并行度以提高性能

    env.add_jars("file:///usr/local/flink/lib/flink-connector-jdbc-1.17.2.jar",
                 "file:///usr/local/flink/lib/mysql-connector-j-8.0.33.jar")

    db_manager.kafka_create_consumer(
        consumer_id="resume-data-source",
        topics=["resumes_topic"]
    )

    # 2. 获取简历数据源
    resumes_source = env.add_source(
        db_manager.kafka_get_flink_consumer('resume-data-source')
    )

    # 3. 对简历数据按 category 进行分区
    partitioned_resumes = resumes_source.key_by(lambda x: json.loads(x).get("category", ""))

    # 4. 处理流数据
    # 简历流 -> 匹配岗位
    resume_matches = partitioned_resumes.map(ResumeMatcher(redis_config))

    # 5. 输出结果（写入 MySQL ）
    # 创建 JdbcSink
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
        .with_batch_size(10)
        .with_max_retries(3)
        .with_batch_interval_ms(100)
        .build()
    )

    # 将结果转换为 ROW 数据类型
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

    # 打印数据流
    matches_stream.print("Debug before JDBC Sink")

    # 添加 JDBC Sink
    matches_stream.add_sink(jdbc_sink)

    print("start to execute job...")

    # 6. 执行作业
    try:
        env.execute("Job-Resume Matching")
    except KeyboardInterrupt:
        print(f"Stop executing job 'Job-Resume Matching'.")
        sys.exit(0)


if __name__ == "__main__":
    main()
