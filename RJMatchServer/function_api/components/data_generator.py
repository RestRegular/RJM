#
# Created by RestRegular on 2025/7/18
#
import random

import pandas as pd
from faker import Faker
from typing import Dict, Any, Tuple, List, Optional

from src.components.error_manager import deprecated

from src.components.database_manager import DataBaseManager

# 初始化Faker
fake = Faker('zh_CN')


@deprecated("DataGenerator 已被弃用，请使用 src.components.data_manager 中的数据构建器生成模拟数据")
class DataGenerator:
    """数据生成器：生成简历和岗位数据"""

    def __init__(self):
        # 定义岗位类别和技能标签
        self.job_categories = {
            "技术": ["Python", "Java", "C++", "JavaScript", "前端开发", "后端开发", "全栈开发", "数据挖掘", "机器学习",
                     "深度学习"],
            "产品": ["产品经理", "产品助理", "需求分析", "用户研究", "产品设计"],
            "运营": ["内容运营", "用户运营", "活动运营", "数据运营", "电商运营"],
            "市场": ["市场营销", "品牌推广", "公关", "市场调研", "广告投放"],
            "销售": ["销售代表", "大客户销售", "渠道销售", "销售管理"],
            "人力资源": ["招聘", "培训", "绩效管理", "薪酬福利", "员工关系"],
            "财务": ["会计", "财务分析", "税务", "审计", "预算管理"]
        }

        # 工作经验分布（年）
        self.experience_distribution = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.experience_weights = [0.2, 0.2, 0.15, 0.15, 0.1, 0.08, 0.05, 0.03, 0.02, 0.01, 0.01]

        # 学历分布
        self.education_distribution = ["大专", "本科", "硕士", "博士"]
        self.education_weights = [0.2, 0.6, 0.15, 0.05]

        # 薪资范围（k）
        self.salary_ranges = {
            0: (5, 10),
            1: (8, 15),
            2: (10, 20),
            3: (15, 25),
            4: (20, 35),
            5: (25, 40),
            6: (30, 50),
            7: (40, 60),
            8: (50, 80),
            9: (60, 100),
            10: (80, 120)
        }

    def generate_resume(self, resume_id: int) -> Dict[str, Any]:
        """生成单份简历数据"""
        category = random.choice(list(self.job_categories.keys()))
        num = random.randint(1, len(self.job_categories[category]))
        skills = random.sample(self.job_categories[category], num)
        experience = random.choices(self.experience_distribution, weights=self.experience_weights)[0]
        education = random.choices(self.education_distribution, weights=self.education_weights)[0]
        salary_range = self.salary_ranges.get(experience, (5, 100))
        expected_salary = random.randint(salary_range[0], salary_range[1])

        resume_ = {
            "resume_id": resume_id,
            "name": fake.name(),
            "age": random.randint(22, 45),
            "gender": random.choice(["男", "女"]),
            "education": education,
            "experience": experience,
            "category": category,
            "skills": skills,
            "expected_salary": expected_salary,
            "location": fake.province()
        }
        return resume_

    def generate_job(self, job_id: int) -> Dict[str, Any]:
        """生成单个岗位数据"""
        category = random.choice(list(self.job_categories.keys()))
        num = random.randint(1, len(self.job_categories[category]))
        skills = random.sample(self.job_categories[category], num)
        required_experience = random.choices(self.experience_distribution, weights=self.experience_weights)[0]
        salary_range = self.salary_ranges.get(required_experience, (5, 100))
        salary_low = random.randint(salary_range[0], salary_range[1] - 5)
        salary_high = random.randint(salary_low + 5, salary_range[1])

        job_ = {
            "job_id": job_id,
            "title": f"{random.choice(['高级', '中级', '初级', '资深', ''])} "
                     f"{category} "
                     f"{random.choice(['专员', '经理', '主管', '工程师', '专家'])}",
            "category": category,
            "company": fake.company(),
            "location": fake.province(),
            "required_skills": skills,
            "required_experience": required_experience,
            "required_education": random.choice(self.education_distribution),
            "salary_low": salary_low,
            "salary_high": salary_high,
            "description": fake.paragraph(nb_sentences=5)
        }
        return job_

    def generate_data(self, num_resumes: int = 10000, num_jobs: int = 100,
                      save_resume_data_to_file: Optional[str] = None,
                      save_job_data_to_file: Optional[str] = None) -> \
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """生成并保存简历和岗位数据"""
        # 生成简历
        resumes_ = [self.generate_resume(i) for i in range(1, num_resumes + 1)]
        if save_resume_data_to_file:
            pd.DataFrame(resumes_).to_json(save_resume_data_to_file, orient="records", force_ascii=False, indent=2)

        # 生成岗位
        jobs_ = [self.generate_job(i) for i in range(1, num_jobs + 1)]
        if save_job_data_to_file:
            pd.DataFrame(jobs_).to_json(save_job_data_to_file, orient="records", force_ascii=False, indent=2)

        print(f"已生成 {num_resumes} 份简历和 {num_jobs} 个岗位数据")
        return resumes_, jobs_


@deprecated("此函数已被弃用，请使用 src.api.resume_job_matcher 中的 RJMatcher.upload_job_datas 方法")
def save_jobs(manager: DataBaseManager, jobs):
    for job in jobs:
        job_id = job["job_id"]
        # 1. 用 Hash 存职位详情（字段级存储，方便后续按需读取）
        hash_key = f"job:info:{job_id}"
        for field, value in job.items():
            manager.redis_hset(hash_key, {field: value})

        # 2. 用 Set 建分类索引（category）
        category = job["category"]
        category_set_key = f"job:category:{category}"
        manager.redis_sadd(category_set_key, job_id)

        # 3. 用 Set 建地区索引（location）
        location = job["location"]
        location_set_key = f"job:location:{location}"
        manager.redis_sadd(location_set_key, job_id)

        # 4. 用 Set 建技能索引（required_skills）
        for skill in job["required_skills"]:
            skill_set_key = f"job:skill:{skill}"
            manager.redis_sadd(skill_set_key, job_id)

        # 5. 用 ZSet 建最低薪资索引（salary_low）
        salary_low = job["salary_low"]
        salary_low_zset_key = "job:salary:low"
        manager.redis_zadd(salary_low_zset_key, {job_id: salary_low})

        # 6. 用 ZSet 建工作经验索引（required_experience）
        experience = job["required_experience"]
        experience_zset_key = "job:experience"
        manager.redis_zadd(experience_zset_key, {job_id: experience})


@deprecated("此函数已被弃用，请使用 src.api.resume_job_matcher 中的 RJMatcher.upload_resume_datas 方法")
def save_resumes(manager: DataBaseManager, resumes):
    for resume in resumes:
        resume_id = resume["resume_id"]
        # 1. 用 Hash 存简历详情（字段级存储，方便后续按需读取）
        hash_key = f"resume:info:{resume_id}"
        for field, value in resume.items():
            manager.redis_hset(hash_key, {field: value})

        # 2. 用 Set 建分类索引（category）
        category = resume["category"]
        category_set_key = f"resume:category:{category}"
        manager.redis_sadd(category_set_key, resume_id)

        # 3. 用 Set 建地区索引（location）
        location = resume["location"]
        location_set_key = f"resume:location:{location}"
        manager.redis_sadd(location_set_key, resume_id)

        # 4. 用 Set 建技能索引（skills）
        for skill in resume["skills"]:
            skill_set_key = f"resume:skill:{skill}"
            manager.redis_sadd(skill_set_key, resume_id)

        # 5. 用 ZSet 建工作经验索引（experience）
        experience = resume["experience"]
        experience_zset_key = "resume:experience"
        manager.redis_zadd(experience_zset_key, {resume_id: experience})

        # 6. 用 ZSet 建期望薪资索引（expected_salary）
        expected_salary = resume["expected_salary"]
        expected_salary_zset_key = "resume:expected_salary"
        manager.redis_zadd(expected_salary_zset_key, {resume_id: expected_salary})


def test():
    # 数据库配置
    mysql_config = {
        'host': 'localhost',
        'database': 'job_matching',
        'user': 'root',
        'password': '322809',
        'charset': 'utf8mb4'
    }

    redis_config = {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'decode_responses': True
    }

    kafka_config = {
        'bootstrap_servers': 'localhost:9092'
    }

    # 1. 初始化数据库连接
    manager = DataBaseManager(mysql_config, redis_config, kafka_config)
    manager.connect()

    # 2. 清空原始数据
    manager.mysql_clear_table(['resumes', 'resume_job_matches', 'jobs'])
    manager.redis_clear()

    # 3. 生成数据
    generator = DataGenerator()
    resumes, jobs = generator.generate_data(num_resumes=100, num_jobs=10)

    # 4. 保存到MySQL
    manager.mysql_insert("resumes", resumes)
    manager.mysql_insert("jobs", jobs)

    # 5. 保存到Redis
    save_jobs(manager, jobs)

    if manager.kafka_exist_topic('resumes_topic'):
        manager.kafka_delete_topic('resumes_topic')
    manager.kafka_create_topic('resumes_topic')

    # 6. 发送到Kafka
    manager.kafka_produce_batch(topic='resumes_topic', messages=[{'value': v} for v in resumes])
    print("已成功发送数据")

    # 7. 关闭数据库连接
    manager.close()


# 示例用法
if __name__ == "__main__":
    test()
