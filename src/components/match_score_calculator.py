#
# Created by RestRegular on 2025/7/23
#
import logging
import re
from datetime import datetime
from typing import Dict, Optional, Any, Tuple, Union

import jieba

from src.components.error_manager import RJMConfigurationError
from src.components.data_manager import ResumeDataBuilder, JobDataBuilder

jieba.default_logger.setLevel(logging.ERROR)
jieba.initialize()

__all__ = [
    'MatchDimensionWeightConfigBuilder',
    'RJMatchDegreeCalculator'
]


class MatchDimensionWeightConfigBuilder:
    """
    匹配维度权重配置建造者类（建造者模式）

    用于构建各匹配维度的权重配置字典，确保：
    1. 所有必填维度的权重均被设置（8个核心维度+其他因素）
    2. 所有权重的总和为1.0（满足归一化要求）
    通过链式调用方法配置各维度权重，最终通过build()方法生成合法的权重配置。
    """

    def __init__(self):
        """初始化建造者实例"""
        self._count = 0  # 已配置的权重维度数量
        # 初始化权重配置字典，包含所有支持的匹配维度，初始值为None（未配置）
        self._weight_config: Dict[str, Optional[float]] = {
            'job_title_match': None,  # 职位名称匹配度权重
            'skill_match': None,  # 技能匹配度权重
            'work_experience_match': None,  # 工作经验匹配度权重
            'location_match': None,  # 地点匹配度权重
            'salary_match': None,  # 薪资匹配度权重
            'education_match': None,  # 学历匹配度权重
            'project_experience_match': None,  # 项目经验匹配度权重
            'other_factors': None  # 其他因素匹配度权重
        }

    def with_job_title_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"职位名称匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['job_title_match'] = weight
        return self

    def with_skill_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"技能匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['skill_match'] = weight
        return self

    def with_work_experience_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"工作经验匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['work_experience_match'] = weight
        return self

    def with_location_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"地点匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['location_match'] = weight
        return self

    def with_salary_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"薪资匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['salary_match'] = weight
        return self

    def with_education_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"学历匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['education_match'] = weight
        return self

    def with_project_experience_match(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"项目经验匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['project_experience_match'] = weight
        return self

    def with_other_factors(self, weight: float) -> 'MatchDimensionWeightConfigBuilder':
        """
        配置"其他因素匹配度"的权重

        Args:
            weight: 权重值（需为非负浮点数，最终总和需为1.0）

        Returns:
            建造者实例自身（支持链式调用）
        """
        self._count += 1
        self._weight_config['other_factors'] = weight
        return self

    def build(self) -> Dict[str, float]:
        """
        构建权重配置字典（最终校验）

        校验逻辑：
        1. 所有维度的权重必须均被设置（无None值）
        2. 所有权重的总和必须为1.0

        Returns:
            合法的权重配置字典（key为维度名称，value为权重值）

        Raises:
            RJMConfigurationError: 当存在未配置的维度或权重总和不为1.0时抛出
        """
        # 检查是否所有维度均已配置
        if self._count != len(self._weight_config):
            missing_fields = [field for field in self._weight_config
                              if self._weight_config[field] is None]
            raise RJMConfigurationError(
                "Match Dimension Weight Config",
                [f"Miss fields: {missing_fields}"]
            )

        # 检查权重总和是否为1.0
        weight_sum = sum(self._weight_config.values())
        if weight_sum != 1.0:
            raise RJMConfigurationError(
                "Match Dimension Weight Config",
                [
                    "Error info: The sum of all weights must be 1.",
                    f"Current weight sum: {weight_sum}"
                ]
            )

        return dict(self._weight_config)

    @classmethod
    def with_default_config(cls) -> 'MatchDimensionWeightConfigBuilder':
        """
        生成默认权重配置的建造者实例

        默认权重分配：
        - 职位名称匹配：0.15
        - 技能匹配：0.25（权重最高，体现技能的核心性）
        - 工作经验匹配：0.20
        - 地点匹配：0.10
        - 薪资匹配：0.10
        - 学历匹配：0.10
        - 项目经验匹配：0.08
        - 其他因素：0.02

        Returns:
            已配置默认权重的建造者实例
        """
        return cls() \
            .with_job_title_match(0.15) \
            .with_skill_match(0.25) \
            .with_work_experience_match(0.20) \
            .with_location_match(0.10) \
            .with_salary_match(0.10) \
            .with_education_match(0.10) \
            .with_project_experience_match(0.08) \
            .with_other_factors(0.02)


class RJMatchDegreeCalculator:
    """
    简历与岗位匹配度计算器

    核心功能：基于配置的权重，计算简历与岗位在多个维度的匹配度，并生成综合评分。
    支持的匹配维度包括：职位名称、技能、工作经验、地点、薪资、学历、项目经验及其他因素。
    """

    _weights: Dict[str, Optional[Any]] = None

    def __init__(self, weight_config_builder: Optional[MatchDimensionWeightConfigBuilder] = None):
        """
        初始化匹配度计算器

        Args:
            weight_config_builder: 权重配置建造者实例（用于获取各维度权重）
        """
        # 初始化各维度权重（通过建造者构建）
        if weight_config_builder:
            self._weights = weight_config_builder.build()
        if not self._weights:
            raise RJMConfigurationError(
                "Match Dimension Weight Config",
                ["还未设置此配置项"]
            )

        # 学历层级映射（用于量化比较，数值越高学历越高）
        self._education_levels = {
            '高中及以下': 1,
            '专科': 2,
            '本科': 3,
            '硕士': 4,
            '博士及以上': 5
        }

        # 技能熟练度映射（未直接使用，预留用于精细化技能匹配）
        self._skill_proficiency = {
            '了解': 1,
            '熟悉': 2,
            '掌握': 3,
            '精通': 4
        }

    @classmethod
    def set_wight_config(cls, weight_config_builder: MatchDimensionWeightConfigBuilder):
        cls._weights = weight_config_builder.build()

    def _calculate_jaccard_similarity(self, list1: list, list2: list) -> float:
        """
        计算两个列表的Jaccard相似度（交集/并集）

        Jaccard相似度公式：J(A,B) = |A∩B| / |A∪B|，取值范围[0,1]
        用于衡量两个集合的重叠程度，值越高表示相似度越高。

        Args:
            list1: 第一个列表（通常为简历关键词列表）
            list2: 第二个列表（通常为岗位关键词列表）

        Returns:
            Jaccard相似度值（0-1之间）
        """
        set1 = set(list1)
        set2 = set(list2)
        intersection = set1.intersection(set2)  # 交集
        union = set1.union(set2)  # 并集
        return len(intersection) / len(union) if union else 0  # 避免除零

    def _extract_keywords(self, text: str) -> list:
        """
        从文本中提取关键词（基础分词与过滤）

        处理逻辑：
        1. 提取文本中的单词（仅保留字母/数字组合）
        2. 转换为小写（统一大小写）
        3. 过滤停用词（无实际意义的常见词汇）
        4. 过滤长度≤1的词（避免无意义字符）

        Args:
            text: 待提取关键词的文本（如职位名称、技能描述等）

        Returns:
            提取后的关键词列表
        """
        if not text:
            return []

        # 停用词列表（中文常见无意义词汇）
        stop_words = {
            '的', '了', '在', '是', '我', '有', '和', '就', '不', '人', '都',
            '一', '一个', '上', '也', '很', '到', '说', '要', '去', '你', '会',
            '着', '没有', '看', '好', '自己', '这'
        }

        content = jieba.lcut(text)

        # 过滤停用词和短词
        return [word for word in content if word not in stop_words and len(word) > 1]

    def _calculate_title_match(self, resume: dict, job: dict) -> float:
        """
        计算职位名称匹配度

        逻辑：
        1. 分别从简历期望职位和岗位名称中提取关键词
        2. 计算关键词的Jaccard相似度
        3. 相似度即为匹配度（0-1之间）

        Args:
            resume: 简历数据字典（需包含 'title' 字段）
            job: 岗位数据字典（需包含 'job_title' 字段）

        Returns:
            职位名称匹配度（0-1之间）
        """
        resume_title = resume.get('title', '')  # 简历中的期望职位
        job_title = job['job_title']  # 岗位名称

        # 提取双方关键词
        resume_keywords = self._extract_keywords(resume_title)
        job_keywords = self._extract_keywords(job_title)

        # 计算相似度
        return self._calculate_jaccard_similarity(resume_keywords, job_keywords)

    def _calculate_skill_match(self, resume: dict, job: dict) -> float:
        """
        计算技能匹配度

        逻辑：
        1. 从岗位要求中提取技能关键词
        2. 遍历简历中的技能，检查是否与岗位技能关键词匹配
        3. 匹配度 = 匹配的技能数 / 岗位技能总关键词数

        Args:
            resume: 简历数据字典（需包含 'skill' 字段，技能列表）
            job: 岗位数据字典（需包含 'requirements' 字段，岗位要求文本）

        Returns:
            技能匹配度（0-1之间）；若岗位无明确技能要求，返回0.5
        """
        resume_skills = resume.get('skill', [])  # 简历中的技能列表
        job_requirements = job.get('requirements', '')  # 岗位要求文本

        # 从岗位要求中提取技能关键词
        job_skill_keywords = self._extract_keywords(job_requirements)
        if not job_skill_keywords:
            return 0.5  # 无明确要求时返回中等分数

        # 统计匹配的技能数量
        match_count = 0
        for skill in resume_skills:
            # 提取单个技能的关键词（如"熟悉JavaScript"提取为["熟悉", "javascript"]）
            skill_keywords = self._extract_keywords(skill)
            # 检查是否有任一关键词匹配岗位技能
            for keyword in skill_keywords:
                if keyword in job_skill_keywords:
                    match_count += 1
                    break  # 一个技能匹配即计数，避免重复

        return match_count / len(job_skill_keywords)

    def _calculate_work_experience_years(self, work_experience: list) -> float:
        """
        从工作经历中计算总工作年限

        处理逻辑：
        1. 解析每条工作经历的时间字符串（如"2020-2023"或"2021至今"）
        2. 提取起始年份和结束年份（"至今"视为当前年份）
        3. 累加各段工作经历的年限（结束年份 - 起始年份）

        Args:
            work_experience: 工作经历列表（每条包含 'time' 字段，时间字符串）

        Returns:
            总工作年限（累加结果）
        """
        total_years = 0
        for exp in work_experience:
            time_str = exp.get('time', '')
            if '-' in time_str:
                parts = time_str.split('-')
                if len(parts) == 2:
                    # 提取起始年份和结束年份
                    start_year = self._extract_year(parts[0])
                    end_year = self._extract_year(parts[1]) or datetime.now().year

                    # 累加有效年限（结束年份 > 起始年份）
                    if start_year and end_year and end_year > start_year:
                        total_years += end_year - start_year
        return total_years

    def _extract_year(self, text: str) -> Optional[int]:
        """
        从文本中提取年份（支持20xx格式的年份）

        Args:
            text: 包含年份的文本（如"2020年"或"至今"）

        Returns:
            提取到的年份（整数）；若未提取到，返回None
        """
        if not text:
            return None
        # 匹配20xx格式的年份（如2020、2023）
        match = re.search(r'\b20\d{2}\b', text)
        return int(match.group()) if match else None

    def _calculate_work_experience_match(self, resume: dict, job: dict) -> float:
        """
        计算工作经验匹配度

        逻辑：
        1. 计算简历中的总工作年限（通过工作经历解析）
        2. 提取岗位要求的最低工作年限（如"3年"提取为3）
        3. 匹配度 = 简历年限 / 岗位要求年限（最高为1.0，即年限达标或超额）

        Args:
            resume: 简历数据字典（需包含'work_experience'字段，工作经历列表）
            job: 岗位数据字典（需包含'minimum_work_time'字段，最低工作时间要求）

        Returns:
            工作经验匹配度（0-1之间）；若岗位无明确要求，返回0.8
        """
        # 计算简历总工作年限
        work_experience = resume['work_experience']
        resume_years = self._calculate_work_experience_years(work_experience)

        # 提取岗位要求的最低工作年限
        min_work_time = job['minimum_work_time']
        job_years_match = re.search(r'(\d+)年', min_work_time)  # 匹配"3年"中的数字
        job_years = int(job_years_match.group(1)) if job_years_match else 0

        if job_years == 0:
            return 0.8  # 无明确要求时返回较高分数

        # 年限达标则为1.0，否则按比例计算
        return min(resume_years / job_years, 1.0)

    def _calculate_location_match(self, resume: dict, job: dict) -> float:
        """
        计算工作地点匹配度

        逻辑（优先级从高到低）：
        1. 若岗位支持远程，返回0.8
        2. 简历地址包含岗位城市，返回1.0
        3. 简历地址包含岗位省份（但不包含城市），返回0.5
        4. 以上均不满足，返回0.2（基础分）

        Args:
            resume: 简历数据字典（需包含 'address' 字段，居住地址）
            job: 岗位数据字典（需包含 'city'、'province'、'job_sort' 字段）

        Returns:
            地点匹配度（0.2/0.5/0.8/1.0四档）
        """
        resume_address = resume.get('address', '').lower()  # 简历地址（小写）
        job_city = job.get('city', '').lower()  # 岗位所在城市
        job_province = job.get('province', '').lower()  # 岗位所在省份
        job_sort = job.get('job_sort', '').lower()  # 岗位类型（是否远程）

        # 远程岗位处理
        if '远程' in job_sort:
            return 0.8

        # 城市匹配
        if job_city in resume_address:
            return 1.0

        # 省份匹配
        if job_province in resume_address:
            return 0.5

        # 不匹配
        return 0.2

    def _parse_salary_range(self, salary_str: str) -> tuple:
        """
        解析薪资范围字符串，提取最低和最高薪资（单位：k）

        支持格式：
        - "18k-25k" → (18, 25)
        - "20k" → (20, 20)
        - 空字符串 → (0, 0)

        Args:
            salary_str: 薪资范围字符串（如简历期望薪资或岗位薪资）

        Returns:
            最低薪资和最高薪资的元组（单位：k）
        """
        if not salary_str:
            return 0, 0

        # 提取字符串中的所有数字
        numbers = re.findall(r'\d+', salary_str)
        if len(numbers) >= 2:
            return int(numbers[0]), int(numbers[1])
        elif len(numbers) == 1:
            return int(numbers[0]), int(numbers[0])
        return 0, 0

    def _calculate_salary_match(self, resume: dict, job: dict) -> float:
        """
        计算薪资期望匹配度

        逻辑：
        1. 解析简历期望薪资范围（min_r, max_r）和岗位薪资范围（min_j, max_j）
        2. 计算薪资区间重叠度：重叠部分长度 / 简历薪资区间长度
        3. 特殊情况处理：
           - 无薪资信息 → 返回0.5
           - 无重叠 → 返回0.3（基础分）

        Args:
            resume: 简历数据字典（需包含 'desc' 字段，包含期望薪资）
            job: 岗位数据字典（需包含 'salary_range' 字段，岗位薪资范围）

        Returns:
            薪资匹配度（0.3-1.0之间）
        """
        # 解析薪资范围
        resume_desc = resume.get('desc', '')  # 简历描述（包含期望薪资）
        job_salary = job['salary_range']  # 岗位薪资范围

        resume_min, resume_max = self._parse_salary_range(resume_desc)
        job_min, job_max = self._parse_salary_range(job_salary)

        # 无薪资信息时返回中等分数
        if (resume_min == 0 and resume_max == 0) or (job_min == 0 and job_max == 0):
            return 0.5

        # 计算薪资区间重叠部分
        overlap_min = max(resume_min, job_min)  # 重叠区间的起始点
        overlap_max = min(resume_max, job_max)  # 重叠区间的结束点

        # 无重叠时返回基础分
        if overlap_min > overlap_max:
            return 0.3

        # 计算重叠比例（重叠长度 / 简历薪资区间长度）
        resume_range = resume_max - resume_min
        if resume_range == 0:
            # 简历薪资为固定值时，若在岗位范围内则返回1.0
            return 1.0 if (overlap_min <= resume_min <= overlap_max) else 0.3

        overlap_ratio = (overlap_max - overlap_min) / resume_range
        return min(overlap_ratio, 1.0)  # 最高为1.0

    def _calculate_education_match(self, resume: dict, job: dict) -> float:
        """
        计算教育背景匹配度（学历+专业）

        逻辑：
        1. 学历匹配（70%权重）：
           - 简历最高学历 ≥ 岗位要求 → 得0.7
           - 否则 → 按比例计算（简历学历层级 / 岗位要求层级 * 0.7）
        2. 专业匹配（30%权重）：
           - 计算简历专业关键词与岗位要求专业关键词的Jaccard相似度
           - 相似度 * 0.3 即为专业得分
        3. 总得分 = 学历得分 + 专业得分

        Args:
            resume: 简历数据字典（需包含 'education' 字段，教育经历列表）
            job: 岗位数据字典（需包含 'requirements' 字段，岗位要求文本）

        Returns:
            教育背景匹配度（0-1之间）
        """
        education = resume.get('education', [])  # 教育经历列表
        job_requirements = job.get('requirements', '').lower()  # 岗位要求（小写）

        # 1. 计算最高学历层级
        max_education_level = 0
        for edu in education:
            level = edu.get('level', '')  # 学历层次（如"本科"、"硕士"）
            # 匹配学历层级（如"本科"对应3）
            for key, val in self._education_levels.items():
                if key in level:
                    if val > max_education_level:
                        max_education_level = val
                    break

        # 2. 分析岗位学历要求层级
        if '博士' in job_requirements:
            job_education_level = 5
        elif '硕士' in job_requirements:
            job_education_level = 4
        elif '本科' in job_requirements:
            job_education_level = 3
        elif '专科' in job_requirements:
            job_education_level = 2
        else:
            job_education_level = 1  # 默认为高中及以下

        # 3. 计算学历得分（70%权重）
        if max_education_level >= job_education_level:
            education_score = 0.7
        else:
            education_score = (max_education_level / job_education_level) * 0.7

        # 4. 计算专业匹配度（30%权重）
        # 提取岗位专业关键词
        job_majors = self._extract_keywords(job_requirements)
        # 提取简历专业关键词
        resume_majors = []
        for edu in education:
            resume_majors.extend(self._extract_keywords(edu.get('major', '')))

        # 计算专业相似度
        major_similarity = self._calculate_jaccard_similarity(resume_majors, job_majors)
        major_score = major_similarity * 0.3

        # 总得分
        return education_score + major_score

    def _calculate_project_experience_match(self, resume: dict, job: dict) -> float:
        """
        计算项目经历匹配度

        逻辑：
        1. 从岗位描述中提取关键词
        2. 遍历简历项目经历，判断是否与岗位关键词相关（相似度>0.2）
        3. 匹配度 = 相关项目数 / 总项目数

        Args:
            resume: 简历数据字典（需包含'project_experience'字段，项目经历列表）
            job: 岗位数据字典（需包含'job_description'字段，岗位描述文本）

        Returns:
            项目经历匹配度（0-1之间）；若无项目经历返回0.3，无岗位描述返回0.5
        """
        projects = resume['project_experience']  # 项目经历列表
        job_description = job['job_description']  # 岗位描述文本

        # 无项目经历时返回基础分
        if not projects:
            return 0.3

        # 提取岗位描述关键词
        job_keywords = self._extract_keywords(job_description)
        if not job_keywords:
            return 0.5  # 无岗位描述时返回中等分

        # 统计相关项目数量
        relevant_projects = 0
        for project in projects:
            # 合并项目信息为文本（标题+描述+角色）
            project_text = f"{project.get('title', '')} {project.get('desc', '')} {project.get('role', '')}"
            project_keywords = self._extract_keywords(project_text)
            # 计算项目与岗位的相似度（>0.2视为相关）
            if self._calculate_jaccard_similarity(project_keywords, job_keywords) > 0.2:
                relevant_projects += 1

        return relevant_projects / len(projects)

    def _calculate_other_factors(self, resume: dict, job: dict) -> float:
        """
        计算其他因素匹配度（自我介绍+奖励经历）

        逻辑：
        1. 自我介绍匹配：自我介绍关键词与岗位描述关键词的Jaccard相似度
        2. 奖励匹配：奖励经历关键词与岗位描述关键词的Jaccard相似度
        3. 总得分 = （自我介绍匹配 + 奖励匹配） / 2

        Args:
            resume: 简历数据字典（需包含 'introduction'、'prize' 字段）
            job: 岗位数据字典（需包含'job_description'字段，岗位描述文本）

        Returns:
            其他因素匹配度（0-1之间）
        """
        # 提取简历信息
        intro = resume.get('introduction', '')  # 自我介绍
        prizes = resume.get('prize', [])  # 奖励经历列表

        # 提取岗位描述关键词
        job_desc = job['job_description']
        job_keywords = self._extract_keywords(job_desc)

        # 1. 自我介绍匹配度
        intro_keywords = self._extract_keywords(intro)
        intro_similarity = self._calculate_jaccard_similarity(intro_keywords, job_keywords)

        # 2. 奖励匹配度
        prize_text = ' '.join([p.get('title', '') for p in prizes])  # 合并奖励文本
        prize_keywords = self._extract_keywords(prize_text)
        prize_relevance = self._calculate_jaccard_similarity(prize_keywords, job_keywords)

        # 平均得分
        return (intro_similarity + prize_relevance) / 2

    def calculate_overall_match(self, resume_builder: ResumeDataBuilder, job_builder: JobDataBuilder) \
            -> Tuple[int, Dict[str, Union[int, float]]]:
        """
        计算简历与岗位的总体匹配度及各维度得分

        流程：
        1. 从数据构建器中获取简历和岗位的字典数据
        2. 计算8个维度的匹配度得分
        3. 计算总体得分（各维度得分 * 对应权重 之和）
        4. 返回总体得分（百分比）和各维度得分字典

        Args:
            resume_builder: 简历数据构建器实例（包含简历数据）
            job_builder: 岗位数据构建器实例（包含岗位数据）

        Returns:
            tuple: (总体匹配度百分比（保留1位小数）, 各维度匹配度字典)
        """
        # 获取简历和岗位的字典数据
        resume = resume_builder.build()
        job = job_builder.build()

        # 计算各维度匹配度
        matches = {
            'job_title_match': round(self._calculate_title_match(resume, job), 2),
            'skill_match': round(self._calculate_skill_match(resume, job), 2),
            'work_experience_match': round(self._calculate_work_experience_match(resume, job), 2),
            'location_match': round(self._calculate_location_match(resume, job), 2),
            'salary_match': round(self._calculate_salary_match(resume, job), 2),
            'education_match': round(self._calculate_education_match(resume, job), 2),
            'project_experience_match': round(self._calculate_project_experience_match(resume, job), 2),
            'other_factors': round(self._calculate_other_factors(resume, job), 2),
        }

        # 计算加权总分（各维度得分 * 权重之和）
        total_score = 0
        for key, weight in self._weights.items():
            total_score += matches[key] * weight

        # 转换为百分比并保留1位小数
        return round(total_score * 100, 1), matches


if __name__ == "__main__":
    # 示例简历数据（模拟实际简历结构）
    sample_resume = {
        "title": "前端开发工程师的简历",
        "desc": "期望薪资18-20k、期望工作城市重庆市、全职",
        "photo": "photo_url",
        "email": "example@mail.com",
        "name": "张三",
        "education": [
            {"school": "北京大学", "major": "计算机科学与技术", "time": "2016-2020", "level": "本科"}
        ],
        "work_experience": [
            {"company": "科技公司A", "job": "前端开发", "desc": "负责公司网站前端开发", "time": "2020-2023"}
        ],
        "project_experience": [
            {"title": "电商平台重构", "desc": "使用React重构电商平台前端", "time": "2021-2022", "role": "核心开发",
             "link": ""}
        ],
        "skill": ["熟悉JavaScript", "精通React", "掌握Vue", "了解Node.js"],
        "introduction": "3年前端开发经验，擅长React框架，团队协作能力强",
        "address": "重庆市朝阳区"
    }

    # 示例岗位数据（模拟实际岗位结构）
    sample_job = {
        "job_eitle": "Web前端开发工程师",
        "company_name": "科技公司B",
        "city": "北京市",
        "province": "北京市",
        "salary_range": "18k-25k",
        "job_description": "负责公司产品前端开发与维护，参与需求分析和技术方案设计",
        "requirements": "本科及以上学历，计算机相关专业，3年以上前端开发经验，熟悉JavaScript、React框架",
        "minimum_work_time": "3年",
        "job_sort": "全职"
    }

    # 计算匹配度示例
    # 1. 创建权重配置（使用默认配置）
    weight_builder = MatchDimensionWeightConfigBuilder.with_default_config()
    # 2. 创建匹配度计算器
    matcher = RJMatchDegreeCalculator(weight_builder)

    # 随机生成的模拟实际数据
    random_resume = ResumeDataBuilder.generate_random_data()
    random_job = JobDataBuilder.generate_random_data()

    from pprint import pprint
    pprint(random_resume.build())
    pprint(random_job.build())
    # 3. 计算匹配度（传入简历和岗位数据构建器）
    overall_score, detailed_matches = matcher.calculate_overall_match(
        random_resume,
        random_job)

    # 输出结果
    print()
    print(f"总体匹配度: {overall_score}%")
    print("\n各维度匹配度:")
    for dimension, score in detailed_matches.items():
        print(f"{dimension}: {round(score * 100, 1)}%")
