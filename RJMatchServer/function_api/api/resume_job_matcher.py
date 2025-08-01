#
# Created by RestRegular on 2025/7/22
#
import re
import json
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable, Union
from unittest.mock import Mock

from pyflink.common.typeinfo import BasicTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema

from function_api.components.error_manager import RJMError
from function_api.components.database_manager import DataBaseManager
from function_api.components.flink_matching_processor import ResumeMatcher, MapFunction, set_match_threshold, JobMatcher


class RJMatcher:
    """
    职位-简历匹配引擎核心类

    该类是职位与简历实时匹配系统的核心协调组件，主要功能包括：
    1. 职位数据管理：批量/单条上传、更新、删除职位数据至Redis，并建立多维度索引（分类/地区/技能等）
    2. 简历数据传输：将简历数据批量发送至Kafka指定主题，支持主题预处理（清空/创建）
    3. 实时匹配调度：基于Flink流处理引擎，实现简历与职位的实时匹配计算
    4. 结果订阅：支持订阅匹配结果，通过回调函数处理实时输出

    核心依赖：
    - Redis：存储职位数据及多维度索引，支持高效条件查询
    - Kafka：简历数据输入通道与匹配结果输出通道
    - Flink：实时流处理引擎，实现简历-职位的分布式匹配计算

    典型工作流：
    1. 初始化实例，配置Redis、Kafka连接信息
    2. 上传职位数据至Redis（upload_job_datas）
    3. 上传简历数据至Kafka（upload_resume_datas）
    4. 配置匹配作业（add_match_job），定义匹配逻辑与Flink参数
    5. 启动匹配任务（start/start_all）
    6. 订阅匹配结果（subscribe_result）
    7. 任务结束后释放资源（close）
    """

    # 存储Flink执行环境的字典，key为作业ID，value为Flink环境实例
    _flink_envs: Dict[str, StreamExecutionEnvironment] = {}

    def __init__(self,
                 redis_config: Dict[str, Any],
                 kafka_config: Dict[str, Any],
                 empty_redis: bool = False):
        """
        初始化RJMatcher实例

        参数:
            redis_config: Redis连接配置字典
                必需键值：
                - host: Redis服务器地址
                - port: 端口号（默认6379）
                - db: 数据库编号
                可选键值：
                - password: 连接密码
                - timeout: 连接超时时间（秒）

            kafka_config: Kafka连接配置字典
                必需键值：
                - bootstrap_servers: Kafka集群地址（格式："host1:port1,host2:port2"）
                - client_id: 客户端标识

            empty_redis: 是否清空 Redis 内容
        """
        # 初始化数据库管理器（封装Redis和Kafka操作）
        self._db_manager = DataBaseManager(
            redis_config=redis_config,
            kafka_config=kafka_config)
        # 初始化Kafka生产者配置（默认序列化器为字符串）
        self._kafka_producer_config = {
            'bootstrap.servers': kafka_config["bootstrap_servers"],
            'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
            'value.serializer': 'org.apache.kafka.common.serialization.StringSerializer'
        }
        # 清空Redis中已有的职位数据（初始化时重置环境）
        if empty_redis: self._db_manager.redis_clear()
        # 存储匹配作业的字典，key为作业ID，value为作业配置
        self._match_jobs: Dict[str, Dict[str, Any]] = {}

    def set_kafka_producer_config(self, **configs):
        """
        更新Kafka生产者配置（覆盖已有配置）

        用于自定义Kafka生产者参数（如批量发送大小、缓冲区等），
        新配置会与已有配置合并，相同键值以新配置为准。

        参数:
            **configs: Kafka生产者配置（如batch_size、linger_ms等）
        """
        self._kafka_producer_config = {
            **self._kafka_producer_config, **configs
        }

    def upload_job_datas(self, jobs: List[Dict[str, Any]]) -> bool:
        """
        批量上传职位数据至Redis并建立多维度索引

        批量处理职位数据，每条数据通过upload_single_job方法存储，
        并为分类、地区、技能等维度建立索引，支持后续高效查询。

        参数:
            jobs: 职位数据列表，每条数据需包含upload_single_job要求的字段

        返回:
            bool: 全部上传成功返回True；任意一条失败返回False

        异常:
            KeyError: 职位数据缺少必需字段
            RedisConnectionError: Redis连接失败
        """
        try:
            for job in jobs:
                self.upload_single_job(job)
            return True
        except KeyError as e:
            print(f"数据验证错误: {e}")
            return False
        except Exception as e:
            print(f"批量上传职位数据失败: {e}")
            return False

    def upload_resume_datas_to_redis(self, resumes: List[Dict[str, Any]]) -> bool:
        try:
            for resume in resumes:
                self.upload_single_resume(resume)
            return True
        except KeyError as e:
            print(f"数据验证错误: {e}")
            return False
        except Exception as e:
            print(f"批量上传简历数据失败: {e}")
            return False

    def upload_resume_datas(self, target_topic: str, resumes: List[Dict[str, Any]], empty_first: bool = False) -> bool:
        """
        批量上传简历数据至Kafka指定主题

        将简历数据序列化为消息发送至Kafka，支持主题预处理（清空旧数据），
        用于后续Flink流处理引擎消费并匹配。

        参数:
            target_topic: 目标Kafka主题名称
            resumes: 简历数据列表，每条需包含：
                - resume_id: 简历唯一标识
                - name: 姓名
                - age: 年龄
                - gender: 性别
                - education: 学历
                - experience: 工作经验（年）
                - category: 意向职位类别
                - skills: 技能列表
                - expected_salary: 期望薪资
                - location: 意向工作地点
            empty_first: 是否先清空主题（True则删除旧主题并重建）

        异常:
            KafkaConnectionError: Kafka连接失败
            KeyError: 简历数据缺少必需字段
        """
        try:
            # 检查主题是否存在
            topic_exists = self._db_manager.kafka_exist_topic(target_topic)
            # 处理主题初始化（清空/创建）
            if empty_first:
                if topic_exists:
                    self._db_manager.kafka_delete_topic(target_topic)
                self._db_manager.kafka_create_topic(target_topic)
            else:
                if not topic_exists:
                    self._db_manager.kafka_create_topic(target_topic)
            # 批量发送简历数据（消息格式：{'value': 简历字典}）
            self._db_manager.kafka_produce_batch(
                topic=target_topic,
                messages=[{'value': r} for r in resumes])
            return True
        except Exception as e:
            print(f"批量上传简历至Kafka失败: {e}")
            return False

    def upload_single_resume(self, resume: Dict[str, Any]) -> bool:
        """
        单条上传简历数据至Redis并建立索引（增量上传）

        验证简历数据完整性后，将数据存储至Redis并构建多维度索引，
        支持后续按岗位类别、地区、技能、工作经验等条件组合查询。

        参数:
            resume: 简历数据字典，需符合Resume模型结构，必需包含模型中的必填字段

        返回:
            bool: 上传成功返回True；失败返回False

        存储逻辑:
            1. 简历详情：Hash结构（键："resume:info:{resume_id}"）
            2. 岗位类别索引：Set结构（键："resume:category:{category}"），存储同类别简历ID
            3. 地区索引：Set结构（键："resume:address:{city}"），按城市分组
            4. 技能索引：Set结构（键："resume:skill:{skill}"），存储具备该技能的简历ID
            5. 工作经验索引：ZSet结构（键："resume:experience"），按总工作年限排序
            6. 最高学历索引：Set结构（键："resume:education:{degree}"），按最高学历分组
            7. 创建时间索引：ZSet结构（键："resume:created"），按创建时间戳排序

        异常:
            KeyError: 缺少必需字段
            RedisConnectionError: Redis连接失败
        """
        # 必需字段校验（基于提供的Resume数据结构调整）
        required_fields = [
            'resume_id', 'category', 'title', 'email', 'name',
            'education', 'work_experience', 'skill', 'address'
        ]

        try:
            # 验证必需字段
            for field in required_fields:
                if field not in resume:
                    raise KeyError(f"简历缺少必需字段: {field}（简历ID: {resume.get('resume_id', '未知')}）")

            resume_id = resume["resume_id"]

            # 数据格式预校验（根据提供的简历结构调整）
            if not isinstance(resume["education"], list):
                raise ValueError(f"education必须是列表类型（简历ID: {resume_id}）")
            for edu in resume["education"]:
                if not isinstance(edu, dict):
                    raise ValueError(f"education列表元素必须是字典（简历ID: {resume_id}）")
                # 根据提供的教育经历结构调整所需字段
                required_edu_fields = ['school', 'level', 'time', 'major']
                for ef in required_edu_fields:
                    if ef not in edu:
                        raise ValueError(f"教育经历缺少字段{ef}（简历ID: {resume_id}）")

            # 1. 处理工作经验（计算总工作年限，根据提供的时间格式调整）
            total_experience = 0.0
            for exp in resume["work_experience"]:
                if "time" in exp:
                    # 处理"2020年10月 - 2024年至今"这种格式
                    time_str = exp["time"]
                    time_parts = time_str.split(' - ')

                    if len(time_parts) != 2:
                        continue  # 时间格式不正确，跳过此条经验

                    start_str, end_str = time_parts

                    # 解析开始日期
                    try:
                        # 处理"2020年10月"格式
                        start_date = datetime.strptime(start_str, "%Y年%m月")
                    except ValueError:
                        try:
                            # 处理"2020年10"格式
                            start_date = datetime.strptime(start_str, "%Y年%m")
                        except ValueError:
                            continue  # 开始日期格式不正确，跳过此条经验

                    # 解析结束日期
                    if end_str == "至今":
                        end_date = datetime.now()
                    else:
                        try:
                            # 处理"2024年至今"格式
                            if "至今" in end_str:
                                end_date = datetime.now()
                            else:
                                # 处理"2020年8"格式
                                end_date = datetime.strptime(end_str, "%Y年%m")
                        except ValueError:
                            try:
                                # 处理"2020年8月"格式
                                end_date = datetime.strptime(end_str, "%Y年%m月")
                            except ValueError:
                                continue  # 结束日期格式不正确，跳过此条经验

                    # 计算工作年限
                    exp_years = (end_date - start_date).days / 365.25
                    total_experience += round(exp_years, 1)

            # 2. 处理最高学历（从education字段提取，使用level字段）
            degree_rank = {"博士": 5, "硕士": 4, "本科": 3, "专科": 2, "高中及以下": 1}
            max_degree = "高中及以下"
            for edu in resume["education"]:
                current_degree = edu["level"]
                if degree_rank.get(current_degree, 0) > degree_rank[max_degree]:
                    max_degree = current_degree

            # 3. 提取城市信息（增强正则表达式以处理更多地址格式）
            city_match = re.search(r'([^省]+[市|区|县]|[^自治区]+自治区|[^直辖市]+直辖市)', resume["address"])
            city = city_match.group(1) if city_match else "未知城市"

            # 4. 存储简历详情（使用JSON序列化复杂字段）
            hash_key = f"resume:info:{resume_id}"
            resume_data = {}
            for k, v in resume.items():
                if isinstance(v, (list, dict)):
                    # 对列表/字典进行JSON序列化
                    resume_data[k] = json.dumps(v, ensure_ascii=False)
                else:
                    resume_data[k] = v  # 普通字段直接存储
            self._db_manager.redis_hset(hash_key, resume_data)

            # 5. 岗位类别索引
            self._db_manager.redis_sadd(f"resume:category:{resume['category']}", resume_id)

            # 6. 地区索引
            self._db_manager.redis_sadd(f"resume:address:{city}", resume_id)

            # 7. 技能索引
            for skill in resume["skill"]:
                normalized_skill = str(skill).strip().lower()
                self._db_manager.redis_sadd(f"resume:skill:{normalized_skill}", resume_id)

            # 8. 工作经验索引
            self._db_manager.redis_zadd("resume:experience", {resume_id: total_experience})

            # 9. 最高学历索引
            self._db_manager.redis_sadd(f"resume:education:{max_degree}", resume_id)

            # 10. 创建时间索引（处理ISO 8601格式）
            if "created_at" in resume:
                try:
                    # 处理"2025-07-30T02:41:45.593148Z"这种格式
                    created_ts = datetime.fromisoformat(resume["created_at"].replace('Z', '+00:00')).timestamp()
                    self._db_manager.redis_zadd("resume:created", {resume_id: created_ts})
                except ValueError as e:
                    print(f"解析创建时间失败: {e}（简历ID: {resume_id}）")

            return True

        except Exception as e:
            print(f"单条简历上传失败: {e}（简历ID: {resume.get('resume_id', '未知')}）")
            return False

    def upload_single_job(self, job: Dict[str, Any]) -> bool:
        """
        单条上传职位数据至Redis并建立索引（增量上传）

        验证职位数据完整性后，将数据存储至Redis并构建多维度索引，
        支持后续按分类、地区、技能等条件组合查询。

        参数:
            job: 职位数据字典，必需包含以下字段：
                - job_id: 职位唯一标识
                - job_category: 职位分类（如"技术开发"）
                - city/province/country: 地区信息（三级地理维度）
                - salary_low/salary_high: 薪资范围（下限/上限）
                - minimum_work_time: 最低工作经验（如"3年"）
                - job_sort: 工作类型（如"全职"）
                - required_skills: 必需技能列表（如["Python", "SQL"]）

        返回:
            bool: 上传成功返回True；失败返回False

        存储逻辑:
            1. 职位详情：Hash结构（键："job:info:{job_id}"）
            2. 分类索引：Set结构（键："job:category:{job_category}"），存储同分类职位ID
            3. 地区索引：三级Set结构（键："job:location:{country/province/city}:{value}"）
            4. 技能索引：Set结构（键："job:skill:{skill}"），存储需该技能的职位ID
            5. 薪资索引：ZSet结构（键："job:salary:low"/"job:salary:high"），按薪资排序
            6. 经验索引：ZSet结构（键："job:experience"），按最低工作经验排序
            7. 工作类型索引：Set结构（键："job:type:{job_sort}"）

        异常:
            KeyError: 缺少必需字段
            RedisConnectionError: Redis连接失败
        """
        # 必需字段校验列表
        required_fields = ['job_id', 'job_category', 'city', 'province', 'country',
                           'salary_low', 'salary_high', 'minimum_work_time',
                           'job_sort', 'required_skills']
        try:
            # 验证必需字段
            for field in required_fields:
                if field not in job:
                    raise KeyError(f"职位缺少必需字段: {field}（职位ID: {job.get('job_id', '未知')}）")

            job_id = job["job_id"]
            # 处理工作经验（转换为数字，如"3年"→3.0）
            experience_str = job["minimum_work_time"]
            try:
                required_experience = float(experience_str.replace('年', '').strip())
            except ValueError:
                # 特殊情况（如"应届毕业生"）视为0年经验
                required_experience = 0.0

            # 1. 存储职位详情（Hash结构）
            hash_key = f"job:info:{job_id}"
            for field, value in job.items():
                self._db_manager.redis_hset(hash_key, {field: value})

            # 2. 分类索引（Set结构）
            category = job["job_category"]
            self._db_manager.redis_sadd(f"job:category:{category}", job_id)

            # 3. 地区索引（三级Set结构）
            self._db_manager.redis_sadd(f"job:location:country:{job['country']}", job_id)
            self._db_manager.redis_sadd(f"job:location:province:{job['province']}", job_id)
            self._db_manager.redis_sadd(f"job:location:city:{job['city']}", job_id)

            # 4. 技能索引（Set结构，技能名称标准化为小写）
            for skill in job["required_skills"]:
                normalized_skill = skill.strip().lower()
                self._db_manager.redis_sadd(f"job:skill:{normalized_skill}", job_id)

            # 5. 薪资索引（ZSet结构）
            self._db_manager.redis_zadd("job:salary:low", {job_id: job["salary_low"]})
            self._db_manager.redis_zadd("job:salary:high", {job_id: job["salary_high"]})

            # 6. 工作经验索引（ZSet结构）
            self._db_manager.redis_zadd("job:experience", {job_id: required_experience})

            # 7. 工作类型索引（Set结构）
            job_type = job["job_sort"]
            self._db_manager.redis_sadd(f"job:type:{job_type}", job_id)
            return True
        except Exception as e:
            print(f"单条职位上传失败: {e}")
            return False

    def delete_jobs(self, *job_ids: Union[str, int]) -> bool:
        """
        从Redis中删除指定职位数据及相关索引

        参数:
            *job_ids: 一个或多个职位ID

        返回:
            bool: 删除成功（或职位不存在）返回True；失败返回False
        """
        try:
            # 删除职位详情Hash表（索引需额外逻辑删除，此处简化实现）
            self._db_manager.redis_delete(*[f"job:info:{job_id}" for job_id in job_ids])
            return True
        except Exception as e:
            print(f"删除职位失败: {e}")
            return False

    def update_job(self, job: Dict[str, Any]) -> bool:
        """
        更新Redis中的职位数据（先删后增）

        参数:
            job: 包含更新后数据的职位字典（必需含job_id）

        返回:
            bool: 更新成功返回True；失败返回False
        """
        job_id = job.get('job_id')
        if not job_id:
            print("更新失败：职位数据缺少job_id")
            return False
        # 先删除旧数据，再插入新数据
        self.delete_jobs(job_id)
        return self.upload_single_job(job)

    def get_job_by_id(self, job_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        根据职位ID查询Redis中的职位详情

        参数:
            job_id: 职位唯一标识

        返回:
            职位字典（含所有字段）；不存在则返回None
        """
        key = f"job:info:{job_id}"
        if self._db_manager.redis_exists(key):
            return self._db_manager.redis_hgetall(key)
        return None

    def get_resume_by_id(self, resume_id):
        resume_data = self._db_manager.redis_hgetall(f"resume:info:{resume_id}")
        # 解析JSON字段
        for field in ['education', 'work_experience', 'skill', 'project_experience']:
            if field in resume_data:
                resume_data[field] = json.loads(resume_data[field])
        return resume_data

    def search_jobs(self,
                    category: Optional[str] = None,
                    location: Optional[str] = None,
                    skills: Optional[List[str]] = None,
                    salary_min: Optional[Union[int, float]] = None,
                    salary_max: Optional[Union[int, float]] = None,
                    experience_min: Optional[Union[int, float]] = None,
                    experience_max: Optional[Union[int, float]] = None,
                    limit: int = 100) -> List[Dict[str, Any]]:
        """
        多条件组合查询职位列表（基于Redis索引高效筛选）

        按分类、地区、技能等条件筛选职位，计算匹配度并排序，支持分页限制。

        参数:
            category: 职位分类（如"技术开发"）；None表示不限制
            location: 工作地点（如"北京"）；None表示不限制
            skills: 必需技能列表；None表示不限制（需匹配所有技能）
            salary_min: 最低薪资；None表示不限制
            salary_max: 最高薪资；None表示不限制
            experience_min: 最低工作经验（年）；None表示不限制
            experience_max: 最高工作经验（年）；None表示不限制
            limit: 最大返回数量（默认100）

        返回:
            按匹配度降序排列的职位列表（含match_score字段）
        """
        try:
            # 1. 分类筛选
            if category:
                category_key = f"job:category:{category}"
                candidate_job_ids = self._db_manager.redis_smembers(category_key)
            else:
                # 无分类筛选时，获取所有职位ID（扫描所有职位详情键）
                job_info_keys = self._db_manager.redis_keys("job:info:*")
                candidate_job_ids = {key.split(":")[-1] for key in job_info_keys}

            # 2. 地区筛选
            if location:
                # 尝试匹配城市→省份→国家级别索引
                location_key = f"job:location:city:{location}"
                if not self._db_manager.redis_exists(location_key):
                    location_key = f"job:location:province:{location}"
                if not self._db_manager.redis_exists(location_key):
                    location_key = f"job:location:country:{location}"
                # 取交集筛选
                location_job_ids = self._db_manager.redis_smembers(location_key)
                candidate_job_ids.intersection_update(location_job_ids)

            if not candidate_job_ids:
                return []  # 无匹配候选，提前返回

            # 3. 技能筛选（需包含所有指定技能）
            if skills:
                for skill in skills:
                    normalized_skill = skill.strip().lower()
                    skill_key = f"job:skill:{normalized_skill}"
                    skill_job_ids = self._db_manager.redis_smembers(skill_key)
                    candidate_job_ids.intersection_update(skill_job_ids)
                    if not candidate_job_ids:
                        return []

            # 4. 工作经验筛选（ZSet范围查询）
            if experience_min is not None or experience_max is not None:
                min_exp = experience_min if experience_min is not None else 0
                max_exp = experience_max if experience_max is not None else float('inf')
                exp_job_ids = self._db_manager.redis_zrangebyscore("job:experience", min_exp, max_exp)
                candidate_job_ids.intersection_update(set(exp_job_ids))
                if not candidate_job_ids:
                    return []

            # 5. 薪资筛选（最低薪资≤期望最高，且最高薪资≥期望最低）
            if salary_min is not None or salary_max is not None:
                min_salary = salary_min if salary_min is not None else 0
                max_salary = salary_max if salary_max is not None else float('inf')
                # 最高薪资≥期望最低
                salary_high_job_ids = self._db_manager.redis_zrangebyscore("job:salary:high", min_salary, float('inf'))
                # 最低薪资≤期望最高
                salary_low_job_ids = self._db_manager.redis_zrangebyscore("job:salary:low", 0, max_salary)
                # 取交集
                salary_job_set = set(salary_high_job_ids) & set(salary_low_job_ids)
                candidate_job_ids.intersection_update(salary_job_set)
                if not candidate_job_ids:
                    return []

            # 6. 计算匹配度并排序
            job_list = []
            for job_id in candidate_job_ids:
                job_info = self.get_job_by_id(job_id)
                if not job_info:
                    continue
                # 基础匹配度为100分，按技能匹配比例调整
                match_score = 100
                if skills:
                    req_skills = job_info.get('required_skills', [])
                    job_skills = set(req_skills.split(',') if isinstance(req_skills, str) else req_skills)
                    matched_skills = job_skills.intersection([s.strip().lower() for s in skills])
                    skill_ratio = len(matched_skills) / len(skills)
                    match_score *= skill_ratio
                job_info['match_score'] = round(match_score, 2)
                job_list.append(job_info)

            # 按匹配度降序排列，限制返回数量
            job_list.sort(key=lambda x: x['match_score'], reverse=True)
            return job_list[:limit]

        except Exception as e:
            print(f"职位查询失败: {e}")
            return []

    def search_resumes(self,
                       category: Optional[str] = None,
                       location: Optional[str] = None,
                       skills: Optional[List[str]] = None,
                       experience_min: Optional[Union[int, float]] = None,
                       experience_max: Optional[Union[int, float]] = None,
                       education: Optional[str] = None,
                       name: Optional[str] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """
        多条件组合查询简历列表（基于Redis索引高效筛选）

        按求职类别、地区、技能等条件筛选简历，计算匹配度并排序，支持结果限制。

        参数:
            category: 求职岗位类别；None表示不限制
            location: 所在地区（城市）；None表示不限制
            skills: 技能列表；None表示不限制（需匹配所有技能）
            experience_min: 最低工作年限；None表示不限制
            experience_max: 最高工作年限；None表示不限制
            education: 最高学历；None表示不限制
            name: 姓名（模糊匹配）；None表示不限制
            limit: 最大返回数量（默认100）

        返回:
            按匹配度降序排列的简历列表（含match_score字段）
        """
        try:
            # 1. 求职类别筛选
            if category:
                category_key = f"resume:category:{category}"
                candidate_resume_ids = self._db_manager.redis_smembers(category_key)
            else:
                # 无类别筛选时，获取所有简历ID
                resume_info_keys = self._db_manager.redis_keys("resume:info:*")
                candidate_resume_ids = {key.split(":")[-1] for key in resume_info_keys}

            if not candidate_resume_ids:
                return []  # 无匹配候选，提前返回

            # 2. 地区筛选（基于城市索引）
            if location:
                # 尝试提取城市名（与上传时的处理逻辑保持一致）
                city_match = re.search(r'([^省]+市|[^自治区]+自治区|[^直辖市]+直辖市)', location)
                city = city_match.group(1) if city_match else location

                location_key = f"resume:address:{city}"
                location_resume_ids = self._db_manager.redis_smembers(location_key)
                candidate_resume_ids.intersection_update(location_resume_ids)

            if not candidate_resume_ids:
                return []

            # 3. 技能筛选（需包含所有指定技能）
            if skills:
                for skill in skills:
                    normalized_skill = skill.strip().lower()
                    skill_key = f"resume:skill:{normalized_skill}"
                    skill_resume_ids = self._db_manager.redis_smembers(skill_key)
                    candidate_resume_ids.intersection_update(skill_resume_ids)
                    if not candidate_resume_ids:
                        return []

            # 4. 工作经验筛选（ZSet范围查询）
            if experience_min is not None or experience_max is not None:
                min_exp = experience_min if experience_min is not None else 0
                max_exp = experience_max if experience_max is not None else float('inf')
                exp_resume_ids = self._db_manager.redis_zrangebyscore("resume:experience", min_exp, max_exp)
                candidate_resume_ids.intersection_update(set(exp_resume_ids))

            if not candidate_resume_ids:
                return []

            # 5. 学历筛选
            if education:
                education_key = f"resume:education:{education}"
                edu_resume_ids = self._db_manager.redis_smembers(education_key)
                candidate_resume_ids.intersection_update(edu_resume_ids)

            if not candidate_resume_ids:
                return []

            # 6. 姓名模糊匹配（需最后处理，性能消耗较高）
            if name:
                filtered_ids = set()
                name_lower = name.lower()
                for resume_id in candidate_resume_ids:
                    # 获取简历详情中的姓名信息
                    resume_info = self.get_resume_by_id(resume_id)
                    if resume_info and name_lower in resume_info.get('name', '').lower():
                        filtered_ids.add(resume_id)
                candidate_resume_ids = filtered_ids

            if not candidate_resume_ids:
                return []

            # 7. 计算匹配度并排序
            resume_list = []
            for resume_id in candidate_resume_ids:
                resume_info = self.get_resume_by_id(resume_id)
                if not resume_info:
                    continue

                # 初始化匹配度为100分
                match_score = 100.0

                # 技能匹配度（占比40%）
                if skills:
                    resume_skills = [s.lower() for s in resume_info.get('skill', [])]
                    matched_skills = set(resume_skills) & set([s.lower() for s in skills])
                    skill_ratio = len(matched_skills) / len(skills) if skills else 1.0
                    match_score *= (0.6 + skill_ratio * 0.4)  # 基础分60% + 技能匹配分40%

                # 工作经验匹配度调整（占比20%）
                if experience_min is not None and experience_max is not None:
                    exp = float(resume_info.get('total_experience', 0))
                    ideal_range = experience_max - experience_min
                    if ideal_range > 0:
                        # 越接近中间值得分越高
                        mid_exp = (experience_min + experience_max) / 2
                        exp_diff = abs(exp - mid_exp) / ideal_range
                        exp_score = max(0, 1 - exp_diff)
                        match_score *= (0.8 + exp_score * 0.2)  # 基础分80% + 经验匹配分20%

                # 学历匹配加分（10分）
                if education:
                    resume_degree = resume_info.get('highest_education', '')
                    if resume_degree == education:
                        match_score += 10

                # 标准化分数到0-100范围
                match_score = min(100.0, max(0.0, match_score))
                resume_info['match_score'] = round(match_score, 2)
                resume_list.append(resume_info)

            # 按匹配度降序排列，限制返回数量
            resume_list.sort(key=lambda x: x['match_score'], reverse=True)
            return resume_list[:limit]

        except Exception as e:
            print(f"简历查询失败: {e}")
            return []

    def add_match_job(self, job_id: str,
                      source_topic: str,
                      flink_config: Dict[str, Any],
                      match_functions: List[MapFunction],
                      stringify_function: Callable[[Any], str],
                      in_debug_mode: bool = True) -> bool:
        """
        添加Flink匹配作业配置

        定义一个匹配任务，包含数据源、Flink配置、匹配逻辑等信息。

        参数:
            job_id: 匹配作业唯一标识（不可重复）
            source_topic: 简历数据来源的Kafka主题
            flink_config: Flink配置字典（如parallelism并行度、lib_jar_path依赖JAR）
            match_functions: 匹配逻辑函数列表（必须为MapFunction子类实例）
            stringify_function: 匹配结果序列化函数（将结果转为字符串）
            in_debug_mode: 是否启用调试模式（打印结果到控制台）

        返回:
            bool: 添加成功返回True；job_id重复返回False
        """
        if job_id in self._match_jobs:
            print(f"添加失败：job_id '{job_id}' 已存在")
            return False
        # 校验匹配函数类型
        for func in match_functions:
            if not isinstance(func, MapFunction):
                raise RJMError(
                    "匹配函数类型错误",
                    [f"实际类型: {type(func)}，期望类型: {MapFunction}"]
                )
        # 存储作业配置
        self._match_jobs[job_id] = {
            'job_id': job_id,
            'flink_config': flink_config,
            'source_topic': source_topic,
            'target_topic': f'target-topic-{job_id}',  # 结果输出主题（自动生成）
            'match_functions': match_functions,
            'stringify_function': stringify_function,
            'in_debug_mode': in_debug_mode
        }
        return True

    def _match(self, match_job: Dict[str, Any]):
        """
        执行Flink匹配任务（内部方法）

        从Kafka读取简历数据，通过Flink流处理执行匹配逻辑，
        结果输出至Kafka目标主题，调试模式下同时打印至控制台。

        参数:
            match_job: 匹配作业配置（由add_match_job定义）
        """
        in_debug_mode = match_job['in_debug_mode']
        job_id = match_job['job_id']
        flink_config = match_job['flink_config']
        # 初始化Flink执行环境
        env = StreamExecutionEnvironment.get_execution_environment()
        self._flink_envs[job_id] = env
        # 设置并行度
        env.set_parallelism(flink_config.get('parallelism', 1))
        # 添加Flink依赖JAR（如Kafka连接器）
        lib_jars = flink_config.get('lib_jar_path', [])
        if lib_jars:
            env.add_jars(*lib_jars if isinstance(lib_jars, list) else [str(lib_jars)])

        # 创建Kafka消费者（从源主题读取简历数据）
        source_topic = match_job["source_topic"]
        source_consumer_id = f"source-consumer-{job_id}"
        self._db_manager.kafka_create_consumer(
            consumer_id=source_consumer_id,
            topics=[source_topic] if isinstance(source_topic, str) else source_topic
        )
        # 从Kafka消费简历数据流
        resumes_source = env.add_source(
            self._db_manager.kafka_get_flink_consumer(source_consumer_id)
        )

        # 按职位类别分区（确保同类数据在同一处理节点）
        partitioned_resumes = resumes_source.key_by(
            lambda x: json.loads(x).get("category", "")  # 按简历意向类别分区
        )

        # 应用匹配函数链（依次执行所有匹配逻辑）
        matched_stream: Optional[DataStream] = None
        for func in match_job['match_functions']:
            matched_stream = partitioned_resumes.map(func)

        # 调试模式：打印结果到控制台
        if in_debug_mode and matched_stream:
            matched_stream.print(f"匹配结果[{job_id}]")

        # 序列化匹配结果（转为字符串）
        if matched_stream:
            matched_stream = matched_stream.flat_map(
                lambda results: [match_job['stringify_function'](r) for r in results],
                output_type=BasicTypeInfo.STRING_TYPE_INFO()
            )

        # 配置KafkaSink（输出结果到目标主题）
        target_topic = match_job['target_topic']
        if self._db_manager.kafka_exist_topic(target_topic):
            self._db_manager.kafka_delete_topic(target_topic)
        self._db_manager.kafka_create_topic(target_topic)
        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers(self._kafka_producer_config['bootstrap.servers']) \
            .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(target_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()
        if matched_stream:
            matched_stream.sink_to(kafka_sink)

        print(f"开始执行匹配作业: [{job_id}]")
        # 执行Flink任务
        env.execute(f"职位-简历匹配任务[{job_id}]")

    def start(self, job_id: str) -> threading.Thread:
        """
        在新线程中启动指定的匹配作业

        参数:
            job_id: 匹配作业ID

        返回:
            已启动的线程对象
        """
        if job_id not in self._match_jobs:
            raise ValueError(f"启动失败：不存在job_id '{job_id}'")
        # 创建并启动线程（守护线程，随主线程退出）
        match_thread = threading.Thread(
            target=self._match,
            args=(self._match_jobs[job_id],),
            name=f"Match-Thread-{job_id}"
        )
        match_thread.daemon = True
        match_thread.start()
        return match_thread

    def start_all(self):
        """启动所有已添加的匹配作业（每个作业一个线程）"""
        for job_id in self._match_jobs:
            self.start(job_id)

    def subscribe_result(self,
                         job_id: Union[str, List[str]],
                         time_out: Optional[int] = 10000,
                         max_message_size: Optional[int] = None,
                         single_message_callback: Callable[[Any], None] = None,
                         end_callback: Callable[[], None] = None,
                         in_debug_mode: bool = True):
        """
        订阅匹配结果（从Kafka目标主题消费消息）

        参数:
            job_id: 要订阅的匹配作业ID（单个或多个）
            time_out: 消费超时时间（毫秒，默认10000）
            max_message_size: 最大消费消息数（None表示持续消费）
            single_message_callback: 单条消息处理函数（接收每条结果）
            end_callback: 消费结束回调函数（无参）
            in_debug_mode: 是否打印调试信息
        """
        # 处理单个job_id
        if isinstance(job_id, str):
            job_ids = [job_id]
        else:
            job_ids = job_id

        for jid in job_ids:
            if jid not in self._match_jobs:
                print(f"订阅失败：job_id '{jid}' 不存在")
                continue
            # 目标主题为作业配置中的target_topic
            target_topic = self._match_jobs[jid]['target_topic']
            consumer_id = f"result-consumer-{jid}"
            # 创建Kafka消费者
            self._db_manager.kafka_create_consumer(
                consumer_id=consumer_id,
                topics=[target_topic],
                auto_offset_reset='latest',  # 从最新消息开始消费
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                key_deserializer=lambda k: str(k)
            )

            # 消息计数（用于控制max_message_size）
            result_count = 0

            def msg_callback(result):
                nonlocal result_count
                result_count += 1
                # 达到最大消息数时停止消费
                if max_message_size and result_count >= max_message_size:
                    self._db_manager.kafka_stop_continuous_consume(consumer_id)
                # 调用单条消息回调
                if single_message_callback:
                    single_message_callback(result)

            # 启动持续消费
            self._db_manager.kafka_start_continuous_consume(
                consumer_id=consumer_id,
                timeout_ms=time_out,
                callback=msg_callback,
                end_callback=end_callback,
                in_debug_mode=in_debug_mode
            )

    def cancel_subscribe_result(self, job_id: str):
        """手动停止指定作业的结果订阅"""
        self._db_manager.kafka_stop_continuous_consume(f"result-consumer-{job_id}")

    def stop_match(self,
                   job_id: str,
                   release_source_topic: bool = True) -> bool:
        """停止指定的Flink匹配作业并释放资源"""
        if job_id not in self._flink_envs:
            print(f"停止失败：不存在job_id '{job_id}'")
            return False
        # 关闭Flink环境，移除作业配置
        self._flink_envs.pop(job_id).close()
        source_topic = self._match_jobs.pop(job_id)['source_topic']
        if release_source_topic:
            self._db_manager.kafka_delete_topic(source_topic)
        print(f"成功停止匹配作业: [{job_id}]")
        return True

    def stop_all(self,
                 release_source_topic: bool = True):
        """停止所有Flink匹配作业"""
        job_id = None
        try:
            for jid in self._flink_envs.keys():
                job_id = jid
                self._flink_envs.pop(job_id).close()
                source_topic = self._match_jobs.pop(job_id)['source_topic']
                if release_source_topic:
                    self._db_manager.kafka_delete_topic(source_topic)
            return True
        except Exception as e:
            print(f"停止作业失败: {job_id}")

    def close(self) -> None:
        """释放所有资源（Redis、Kafka、Flink）"""
        self._db_manager.close()  # 关闭数据库连接
        # 关闭所有Flink环境
        for env in self._flink_envs.values():
            env.close()
        self._flink_envs.clear()
        self._match_jobs.clear()  # 清空作业配置

    # 以下为配置获取方法
    def get_redis_config(self) -> Dict[str, Any]:
        return self._db_manager.get_redis_config()

    def get_kafka_config(self) -> Dict[str, Any]:
        return self._db_manager.get_kafka_config()

    def get_match_job_flink_config(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self._match_jobs.get(job_id, {}).get('flink_config')

    def all_match_job_ids(self) -> List[str]:
        return list(self._match_jobs.keys())


def main():
    """主函数：演示RJMatcher的完整使用流程"""
    from .components.data_manager import ResumeDataBuilder, JobDataBuilder

    def generate_simulate_data(resume_num: int, job_num: int):
        """生成模拟的简历和职位数据"""
        resumes_ = [ResumeDataBuilder.generate_random_data().build() for _ in range(resume_num)]
        jobs_ = [JobDataBuilder.generate_random_data().build() for _ in range(job_num)]
        return resumes_, jobs_

    # 1. 初始化匹配引擎（配置Redis和Kafka）
    matcher = RJMatcher(
        redis_config={
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'decode_responses': True
        },
        kafka_config={
            'bootstrap_servers': 'localhost:9092',
            'client_id': 'job-matching-client'
        })
    # 配置Kafka生产者参数
    matcher.set_kafka_producer_config(batch_size=16384, buffer_memory=33554432)
    # 设置匹配阈值
    set_match_threshold(20)

    # 2. 生成并上传测试数据
    resumes, jobs = generate_simulate_data(resume_num=10, job_num=10)
    matcher.upload_job_datas(jobs)  # 上传职位至Redis
    # 上传简历至两个Kafka主题（清空旧数据）
    matcher.upload_resume_datas('source-topic-1', resumes, empty_first=True)
    matcher.upload_resume_datas('source-topic-2', resumes, empty_first=True)

    # 3. 添加匹配作业
    # 作业1：基于简历匹配职位
    matcher.add_match_job(
        job_id="test_resume_match_process",
        source_topic="source-topic-2",
        flink_config={'parallelism': 5},
        match_functions=[ResumeMatcher(redis_config=matcher.get_redis_config())],
        stringify_function=lambda result: json.dumps(list(result))
    )
    # 作业2：基于指定职位匹配简历
    matcher.add_match_job(
        job_id="test_job_match_process",
        source_topic="source-topic-1",
        flink_config={'parallelism': 5},
        match_functions=[JobMatcher(redis_config=matcher.get_redis_config(), job_ids=list(range(1, 10, 2)))],
        stringify_function=lambda result: json.dumps(list(result))
    )

    # 4. 启动所有匹配作业
    matcher.start_all()

    # 5. 订阅匹配结果
    results = []  # 存储所有结果

    def handle_single_result(result):
        """处理单条匹配结果"""
        nonlocal results
        results.append(result['value'])
        print(f"收到结果: {result['value']}")

    def handle_all_results():
        """所有结果处理完成后执行"""
        print(f"\n所有匹配结果: {results}")

    # 订阅作业2的结果（超时1000ms，持续消费）
    matcher.subscribe_result(
        job_id="test_job_match_process",
        time_out=1000,
        max_message_size=None,  # 不限制消息数
        single_message_callback=handle_single_result,
        end_callback=handle_all_results,
        in_debug_mode=False
    )

    # 等待Flink任务执行（20秒）
    time.sleep(20)

    # 6. 释放资源
    matcher.close()


def test_search_jobs():
    """测试search_jobs方法的各种查询场景"""
    # 创建模拟数据库管理器
    mock_db = Mock()

    # 初始化匹配引擎并替换数据库管理器
    matcher = RJMatcher(
        redis_config={'host': 'localhost', 'port': 6379, 'db': 0},
        kafka_config={'bootstrap_servers': 'localhost:9092', 'client_id': 'test'}
    )
    matcher._db_manager = mock_db
    matcher.get_job_by_id = Mock()  # 模拟职位查询

    # 测试数据
    test_jobs = [
        {
            "job_id": "1", "job_title": "Python开发工程师", "job_category": "技术开发",
            "city": "北京", "required_skills": ["Python", "SQL"],
            "salary_low": 20, "salary_high": 30, "minimum_work_time": "3年"
        },
        {
            "job_id": "2", "job_title": "Java开发工程师", "job_category": "技术开发",
            "city": "上海", "required_skills": ["Java", "Spring"],
            "salary_low": 25, "salary_high": 35, "minimum_work_time": "5年"
        },
        {
            "job_id": "3", "job_title": "人力资源经理", "job_category": "人力资源",
            "city": "北京", "required_skills": ["员工关系", "培训"],
            "salary_low": 15, "salary_high": 25, "minimum_work_time": "5年"
        }
    ]

    # 配置模拟方法返回值
    def get_job_side_effect(job_id):
        for job in test_jobs:
            if job["job_id"] == job_id:
                return job
        return None

    matcher.get_job_by_id.side_effect = get_job_side_effect

    # 模拟Redis操作返回值
    mock_db.redis_smembers.side_effect = [
        {"1", "2"}, {"1", "3"}, {"1"}, {"1", "2", "3"}, {"1"}, set(), set()
    ]
    mock_db.redis_zrangebyscore.return_value = ["1", "2", "3"]
    mock_db.redis_keys.return_value = ["job:info:1", "job:info:2", "job:info:3"]
    mock_db.redis_exists.return_value = True

    # 测试场景
    # 场景1：按分类"技术开发"查询
    result1 = matcher.search_jobs(category="技术开发")
    assert len(result1) == 2, "场景1失败：技术开发分类应返回2个职位"

    # 场景2：按地点"北京"查询
    result2 = matcher.search_jobs(location="北京")
    assert len(result2) == 2, "场景2失败：北京地区应返回2个职位"

    # 场景3：按技能"Python"查询
    result3 = matcher.search_jobs(skills=["Python"])
    assert len(result3) == 1 and result3[0]["job_id"] == "1", "场景3失败：Python技能应匹配职位1"

    # 场景4：多条件组合查询（技术开发+北京+薪资20-30）
    result4 = matcher.search_jobs(
        category="技术开发", location="北京", salary_min=20, salary_max=30
    )
    assert len(result4) == 1 and result4[0]["job_id"] == "1", "场景4失败：多条件应匹配职位1"

    # 场景5：无匹配结果
    result5 = matcher.search_jobs(category="市场营销", location="广州")
    assert len(result5) == 0, "场景5失败：无匹配应返回空列表"

    print("所有查询测试场景均通过！")


if __name__ == '__main__':
    # 运行测试或主函数
    # test_search_jobs()
    main()
