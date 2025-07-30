#
# Created by RestRegular on 2025/7/23
#
import random
import re
from typing import List, Dict, Any, Optional, Union, Set

from faker import Faker
from src.components.error_manager import ResumeValidationError, JobValidationError

__all__ = [
    'ResumeDataBuilder',
    'JobDataBuilder',
    'convert_camel_case_to_underscore_case'
]

# 定义支持的职位类别列表
_job_categories = [
    "技术", "产品", "运营", "市场", "销售", "人力资源", "财务"
]

# 为每个职位类别定义相关技能列表，用于数据生成和验证
_job_category_skills = {
    "技术": ["Python", "Java", "C++", "JavaScript", "前端开发", "后端开发", "全栈开发", "数据挖掘", "机器学习",
             "深度学习"],
    "产品": ["产品经理", "产品助理", "需求分析", "用户研究", "产品设计"],
    "运营": ["内容运营", "用户运营", "活动运营", "数据运营", "电商运营"],
    "市场": ["市场营销", "品牌推广", "公关", "市场调研", "广告投放"],
    "销售": ["销售代表", "大客户销售", "渠道销售", "销售管理"],
    "人力资源": ["招聘", "培训", "绩效管理", "薪酬福利", "员工关系"],
    "财务": ["会计", "财务分析", "税务", "审计", "预算管理"]
}


def convert_camel_case_to_underscore_case(name: str) -> str:
    """
    将驼峰式命名转换为下划线命名

    用于将字典中的键名（通常为驼峰式）转换为构建器类中with_xxx方法的命名格式
    例如: "work_experience" -> "work_experience"，最终对应with_work_experience方法

    Args:
        name: 驼峰式命名的字符串

    Returns:
        转换后的下划线命名字符串
    """
    method_name = ""
    for i, char in enumerate(name):
        if char.isupper() and i > 0:
            method_name += '_' + char.lower()
        else:
            method_name += char.lower()
    return method_name.strip()


class ResumeDataBuilder:
    """
    简历数据构建器类

    提供链式调用的API用于构建和验证简历数据，确保数据格式正确且完整。
    支持从字典初始化数据，并通过build()方法获取最终验证后的简历数据。
    采用建造者模式，使简历数据的构建过程更加灵活和可控。
    """

    # 类级变量，用于自动生成唯一简历ID
    _id = 0
    _ids: Set[int] = set()

    def __init__(self):
        """初始化简历数据结构

        定义简历包含的所有字段及其初始值，None表示待填充，列表字段初始化为空列表
        包含个人基本信息、教育经历、工作经历、项目经历、技能等完整简历要素
        """
        self._data: Dict[str, Union[str, int, List, None]] = {
            "resume_id": None,  # 简历ID（唯一标识）
            "category": None,  # 求职岗位类别
            "title": None,  # 简历标题
            "desc": None,  # 简历描述（期望薪资、工作城市等）
            "photo": None,  # 照片URL
            "email": None,  # 联系邮箱
            "name": None,  # 姓名
            "education": [],  # 教育经历列表
            "work_experience": [],  # 工作经历列表
            "project_experience": [],  # 项目经历列表
            "skill": [],  # 技能列表
            "introduction": None,  # 个人介绍
            "address": None  # 联系地址
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ResumeDataBuilder':
        """
        从字典数据构建ResumeDataBuilder对象

        遍历字典中的键值对，自动调用对应的with_xxx方法进行数据填充
        仅处理存在对应with_xxx方法的键值对，实现从字典到构建器的快速转换

        Args:
            data: 包含简历信息的字典，键应为驼峰式命名（如"work_experience"）

        Returns:
            初始化后的ResumeDataBuilder实例
        """
        builder = cls()
        for key, value in data.items():
            # 转换键名为下划线格式并拼接with_前缀，得到方法名
            method_name = 'with_' + convert_camel_case_to_underscore_case(key)
            # 检查方法是否存在且可调用
            if hasattr(builder, method_name) and callable(getattr(builder, method_name)):
                # 调用对应的with_xxx方法设置值
                getattr(builder, method_name)(value)
        return builder

    def with_resume_id(self, value: int = None) -> 'ResumeDataBuilder':
        """
        设置简历ID，若不提供则自动生成唯一ID

        Args:
            value: 简历ID值，为None时自动生成

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若ID重复
        """
        if not value:
            ResumeDataBuilder._id += 1
            value = ResumeDataBuilder._id
            if value in ResumeDataBuilder._ids:
                raise ResumeValidationError("resume_id",
                                            ["简历ID不能重复",
                                             f"Duplicate id: {value}"])
            ResumeDataBuilder._ids.add(value)
        self._data['resume_id'] = value
        return self

    def with_category(self, value: str) -> 'ResumeDataBuilder':
        """
        设置求职岗位类别，必须是预定义类别之一

        Args:
            value: 岗位类别字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若类别不在预定义列表中
        """
        if value not in _job_categories:
            raise ResumeValidationError("category",
                                        ["职位类别仅包含以下类别: [" +
                                         ', '.join(["'" + c + "'" for c in _job_categories]) + "]",
                                         f"Error category: {value}"])
        self._data['category'] = value
        return self

    def with_title(self, value: str) -> 'ResumeDataBuilder':
        """
        设置简历标题

        Args:
            value: 简历标题字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型
        """
        if not isinstance(value, str):
            raise ResumeValidationError("title", "标题必须是字符串类型")
        self._data["title"] = value
        return self

    def with_desc(self, value: str) -> 'ResumeDataBuilder':
        """
        设置简历描述（通常包含期望薪资、意向工作城市等信息）

        Args:
            value: 描述内容字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型
        """
        if not isinstance(value, str):
            raise ResumeValidationError("desc", "描述必须是字符串类型")
        self._data["desc"] = value
        return self

    def with_photo(self, value: str) -> 'ResumeDataBuilder':
        """
        设置个人照片的URL地址

        Args:
            value: 照片的URL字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型
        """
        if not isinstance(value, str):
            raise ResumeValidationError("photo", "照片URL必须是字符串类型")
        self._data["photo"] = value
        return self

    def with_email(self, value: str) -> 'ResumeDataBuilder':
        """
        设置联系邮箱，简单验证格式（包含@符号）

        Args:
            value: 邮箱地址字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型或不包含@符号
        """
        if not isinstance(value, str) or "@" not in value:
            raise ResumeValidationError("email", "邮箱格式不正确")
        self._data["email"] = value
        return self

    def with_name(self, value: str) -> 'ResumeDataBuilder':
        """
        设置个人姓名，不能为空

        Args:
            value: 姓名字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise ResumeValidationError("name", "姓名不能为空字符串")
        self._data["name"] = value
        return self

    def with_education(self, value: List[Dict[str, str]]) -> 'ResumeDataBuilder':
        """
        设置教育经历

        每个教育经历项必须包含学校、专业、时间和学历层次信息

        Args:
            value: 教育经历列表，每个元素为包含教育信息的字典

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError:
                - 若输入不是列表类型
                - 列表中包含非字典元素
                - 字典缺少必要字段（school, major, time, level）
        """
        if not isinstance(value, list):
            raise ResumeValidationError("education", "教育经历必须是列表类型")

        # 教育经历必须包含的字段
        required_fields = ["school", "major", "time", "level"]
        for item in value:
            if not isinstance(item, dict):
                raise ResumeValidationError("education", "教育经历项必须是字典类型")
            for field in required_fields:
                if field not in item:
                    raise ResumeValidationError("education", f"教育经历缺少必要字段: {field}")

        self._data["education"] = value
        return self

    def with_work_experience(self, value: List[Dict[str, str]]) -> 'ResumeDataBuilder':
        """
        设置工作经历

        每个工作经历项必须包含公司名称、职位、描述和时间信息

        Args:
            value: 工作经历列表，每个元素为包含工作信息的字典

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError:
                - 若输入不是列表类型
                - 列表中包含非字典元素
                - 字典缺少必要字段（company, job, desc, time）
        """
        if not isinstance(value, list):
            raise ResumeValidationError("work_experience", "工作经历必须是列表类型")

        # 工作经历必须包含的字段
        required_fields = ["company", "job", "desc", "time"]
        for item in value:
            if not isinstance(item, dict):
                raise ResumeValidationError("work_experience", "工作经历项必须是字典类型")
            for field in required_fields:
                if field not in item:
                    raise ResumeValidationError("work_experience", f"工作经历缺少必要字段: {field}")

        self._data["work_experience"] = value
        return self

    def with_project_experience(self, value: List[Dict[str, str]]) -> 'ResumeDataBuilder':
        """
        设置项目经历

        每个项目经历项必须包含项目名称、描述、时间和角色信息

        Args:
            value: 项目经历列表，每个元素为包含项目信息的字典

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError:
                - 若输入不是列表类型
                - 列表中包含非字典元素
                - 字典缺少必要字段（title, desc, time, role）
        """
        if not isinstance(value, list):
            raise ResumeValidationError("project_experience", "项目经历必须是列表类型")

        # 项目经历必须包含的字段
        required_fields = ["title", "desc", "time", "role"]
        for item in value:
            if not isinstance(item, dict):
                raise ResumeValidationError("project_experience", "项目经历项必须是字典类型")
            for field in required_fields:
                if field not in item:
                    raise ResumeValidationError("project_experience", f"项目经历缺少必要字段: {field}")

        self._data["project_experience"] = value
        return self

    def with_skill(self, value: List[str]) -> 'ResumeDataBuilder':
        """
        设置技能列表

        技能列表中的每个元素都必须是字符串类型

        Args:
            value: 技能字符串组成的列表

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError:
                - 若输入不是列表类型
                - 列表中包含非字符串元素
        """
        if not isinstance(value, list):
            raise ResumeValidationError("skill", "技能必须是列表类型")
        for skill in value:
            if not isinstance(skill, str):
                raise ResumeValidationError("skill", "技能列表中的每项必须是字符串")
        self._data["skill"] = value
        return self

    def with_introduction(self, value: str) -> 'ResumeDataBuilder':
        """
        设置个人介绍

        Args:
            value: 个人介绍字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型
        """
        if not isinstance(value, str):
            raise ResumeValidationError("introduction", "个人介绍必须是字符串类型")
        self._data["introduction"] = value
        return self

    def with_address(self, value: str) -> 'ResumeDataBuilder':
        """
        设置联系地址

        Args:
            value: 地址字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            ResumeValidationError: 若输入不是字符串类型
        """
        if not isinstance(value, str):
            raise ResumeValidationError("address", "地址必须是字符串类型")
        self._data["address"] = value
        return self

    def build(self) -> Dict[str, Any]:
        """
        构建并返回验证后的简历数据

        检查所有必要字段是否已填充，确保没有缺失的必填项

        Returns:
            包含完整简历信息的字典（数据副本，避免外部修改）

        Raises:
            ResumeValidationError: 若存在未填充的必要字段
        """
        # 验证所有字段是否已设置（没有None值）
        required_fields = self._data.keys()
        missing_fields = [field for field in required_fields if self._data[field] is None]
        if missing_fields:
            raise ResumeValidationError(
                "build",
                ["缺少必要字段: [" + ', '.join(["'" + f + "'" for f in missing_fields]) + "]"])
        # 返回数据副本，防止外部意外修改内部数据
        return dict(self._data)

    @staticmethod
    def generate_random_data() -> 'ResumeDataBuilder':
        """
        生成随机模拟的简历数据

        使用Faker库生成符合格式要求的随机简历信息，包含所有必要字段，
        可直接用于测试或演示。

        Returns:
            已填充随机数据的ResumeDataBuilder实例
        """
        fake = Faker("zh_CN")  # 使用中文环境生成模拟数据
        builder = ResumeDataBuilder()

        # 生成基础信息
        name = fake.name()
        email = fake.email()
        address = fake.address()
        category = random.choice(_job_categories)

        # 生成简历标题和描述
        job_titles = ["软件工程师", "产品经理", "数据分析师", "UI设计师", "市场营销专员"]
        title = f"{name}的{random.choice(job_titles)}简历"

        cities = ["北京", "上海", "广州", "深圳", "杭州", "成都"]
        salary_ranges = ["8k-12k", "12k-18k", "18k-25k", "25k-35k", "35k+"]
        desc = f"期望工作城市：{random.choice(cities)} | 期望薪资：{random.choice(salary_ranges)} | 求职状态：随时到岗"

        # 生成教育经历（1-3条）
        education: List[Dict[str, str]] = []
        education_levels = ["本科", "硕士", "博士", "专科"]
        majors = ["计算机科学与技术", "软件工程", "电子信息工程", "市场营销", "人力资源管理", "会计学"]
        universities = [
            "北京大学", "清华大学", "复旦大学", "上海交通大学",
            "浙江大学", "南京大学", "武汉大学", "中山大学",
            "四川大学", "哈尔滨工业大学", "西安交通大学", "华中科技大学"
        ]

        for _ in range(random.randint(1, 3)):
            start_year = random.randint(2005, 2020)
            end_year = start_year + random.randint(3, 6)
            education.append({
                "school": random.choice(universities),
                "major": random.choice(majors),
                "time": f"{start_year}年9月 - {end_year}年6月",
                "level": random.choice(education_levels)
            })

        # 按时间排序教育经历
        education.sort(key=lambda x: x["time"])

        # 生成工作经历（1-5条）
        work_experience: List[Dict[str, str]] = []
        companies = [fake.company() for _ in range(5)]
        positions = ["高级工程师", "项目经理", "产品经理", "技术负责人", "设计师", "市场主管"]

        for _ in range(random.randint(1, 5)):
            start_year = random.randint(2010, 2023)
            start_month = random.randint(1, 12)
            end_year = random.randint(start_year, 2024)
            end_month = random.randint(1, 12) if end_year != 2024 else "至今"

            work_experience.append({
                "company": random.choice(companies),
                "job": random.choice(positions),
                "desc": fake.paragraph(nb_sentences=3),  # 生成3句描述
                "time": f"{start_year}年{start_month}月 - {end_year}年{end_month}"
            })

        # 按时间倒序排列工作经历
        work_experience.sort(key=lambda x: x["time"], reverse=True)

        # 生成项目经历（1-4条）
        project_experience: List[Dict[str, str]] = []
        project_types = ["电商平台", "企业管理系统", "移动应用", "数据分析平台", "内容管理系统"]

        for _ in range(random.randint(1, 4)):
            start_year = random.randint(2015, 2023)
            start_month = random.randint(1, 12)
            end_year = random.randint(start_year, 2024)
            end_month = random.randint(1, 12) if end_year != 2024 else "至今"

            project_experience.append({
                "title": f"{random.choice(project_types)}开发",
                "desc": fake.paragraph(nb_sentences=4),  # 生成4句描述
                "time": f"{start_year}年{start_month}月 - {end_year}年{end_month}",
                "role": random.choice(["负责人", "核心开发", "参与者", "技术顾问"])
            })

        # 从对应类别中随机选择技能
        skills = random.sample(_job_category_skills[category], random.randint(1, len(_job_category_skills[category])))

        # 生成个人介绍
        introduction = fake.paragraph(nb_sentences=5)

        # 组装所有数据
        return (builder
                .with_resume_id()
                .with_category(category)
                .with_title(title)
                .with_desc(desc)
                .with_photo(f"https://picsum.photos/seed/{name}/200/200")  # 随机头像
                .with_email(email)
                .with_name(name)
                .with_education(education)
                .with_work_experience(work_experience)
                .with_project_experience(project_experience)
                .with_skill(skills)
                .with_introduction(introduction)
                .with_address(address))


class JobDataBuilder:
    """
    职位信息构建器类

    提供链式调用的API用于构建和验证职位信息，确保数据格式正确且完整。
    支持从字典初始化数据，并通过build()方法获取最终验证后的职位数据。
    采用建造者模式，使职位数据的构建过程更加灵活和可控。
    """

    # 类级变量，用于自动生成唯一职位ID
    _id = 0
    _ids: Set[int] = set()

    def __init__(self):
        """初始化职位信息数据结构

        定义职位包含的所有字段及其初始值（均为None，表示待填充）
        包含职位基本信息、薪资、要求、描述等完整职位要素
        """
        self._data: Dict[str, Union[List[str], str, int, None]] = {
            "job_id": None,  # 职位ID
            "job_title": None,  # 职位名称
            "company_name": None,  # 公司名称
            "city": None,  # 工作城市
            "province": None,  # 所在省份
            "country": None,  # 所在国家
            "salary_range": None,  # 薪资范围
            "salary_low": None,  # 薪资下限（k）
            "salary_high": None,  # 薪资上限（k）
            "job_description": None,  # 职位描述
            "requirements": None,  # 职位要求
            "job_category": None,  # 职位类别
            "minimum_work_time": None,  # 最低工作年限
            "job_sort": None,  # 工作类型（全职/兼职等）
            "required_skills": None,  # 要求的技能
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobDataBuilder':
        """
        从字典数据构建JobDataBuilder对象

        遍历字典中的键值对，自动调用对应的with_xxx方法进行数据填充
        仅处理存在对应with_xxx方法的键值对，实现从字典到构建器的快速转换

        Args:
            data: 包含职位信息的字典，键应为驼峰式命名（如"job_title"）

        Returns:
            初始化后的JobDataBuilder实例
        """
        builder = cls()
        # 遍历字典中的键值对，调用对应的with_xxx方法
        for key, value in data.items():
            # 转换键名为下划线格式并拼接with_前缀，得到方法名
            method_name = 'with_' + convert_camel_case_to_underscore_case(key)
            # 检查方法是否存在且可调用
            if hasattr(builder, method_name) and callable(getattr(builder, method_name)):
                # 调用对应的with_xxx方法设置值
                getattr(builder, method_name)(value)
        return builder

    def with_job_id(self, value: Optional[int] = None) -> 'JobDataBuilder':
        """
        设置职位ID，若不提供则自动生成唯一ID

        Args:
            value: 职位ID值，为None时自动生成

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若ID重复
        """
        if value is None:
            JobDataBuilder._id += 1
            value = JobDataBuilder._id
            if value in JobDataBuilder._ids:
                raise JobValidationError("job_id",
                                         ["职位ID不能重复",
                                          f"Duplicate id: {value}"])
            JobDataBuilder._ids.add(value)
        self._data['job_id'] = value
        return self

    def with_job_title(self, value: str) -> 'JobDataBuilder':
        """
        设置职位名称，不能为空

        Args:
            value: 职位名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("job_title", "职位名称不能为空字符串")
        self._data["job_title"] = value
        return self

    def with_company_name(self, value: str) -> 'JobDataBuilder':
        """
        设置公司名称，不能为空

        Args:
            value: 公司名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("company_name", "公司名称不能为空字符串")
        self._data["company_name"] = value
        return self

    def with_country(self, value: str) -> 'JobDataBuilder':
        """
        设置所在国家，不能为空

        Args:
            value: 国家名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("country", "国家名称不能为空字符串")
        self._data["country"] = value
        return self

    def with_city(self, value: str) -> 'JobDataBuilder':
        """
        设置工作城市，不能为空

        Args:
            value: 城市名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("city", "城市名称不能为空字符串")
        self._data["city"] = value
        return self

    def with_province(self, value: str) -> 'JobDataBuilder':
        """
        设置所在省份，不能为空

        Args:
            value: 省份名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("province", "省份名称不能为空字符串")
        self._data["province"] = value
        return self

    def with_salary_range(self, value: str) -> 'JobDataBuilder':
        """
        设置薪资范围，同时提取薪资下限和上限

        格式必须为"xxk-yyk"（如"18k-25k"），自动解析出薪资上下限

        Args:
            value: 薪资范围字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError:
                - 若输入不是字符串类型或为空字符串
                - 格式不正确（不包含'-'或不符合xxk-yyk格式）
                - 薪资下限大于上限
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("salary_range", "薪资范围不能为空字符串")

        pattern = r'^(\d+)k-(\d+)k$'
        match = re.match(pattern, value)

        if not match or len(match.groups()) != 2:
            raise JobValidationError("salary_range", [
                "薪资范围格式不匹配，正确格式应为 'xxk-yyk'",
                f"错误格式数据: {value}"
            ])

        # 提取薪资下限和上限（转换为整数，单位为k）
        salary_low = int(match.group(1))
        salary_high = int(match.group(2))

        if salary_low > salary_high:
            raise JobValidationError("salary_range", [
                "前半部分薪资应小于后半部分薪资",
                f"错误格式数据: {value}"
            ])

        # 存储原始范围和拆分后的上下限
        self._data["salary_range"] = value
        self._data["salary_low"] = salary_low
        self._data["salary_high"] = salary_high

        return self

    def with_job_description(self, value: str) -> 'JobDataBuilder':
        """
        设置职位描述，不能为空

        Args:
            value: 职位描述字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("job_description", "职位描述不能为空字符串")
        self._data["job_description"] = value
        return self

    def with_requirements(self, value: str) -> 'JobDataBuilder':
        """
        设置职位要求，不能为空

        Args:
            value: 职位要求字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("requirements", "职位要求不能为空字符串")
        self._data["requirements"] = value
        return self

    def with_minimum_work_time(self, value: str) -> 'JobDataBuilder':
        """
        设置最低工作年限，格式必须包含'年'（如"3年"）

        Args:
            value: 最低工作年限字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError:
                - 若输入不是字符串类型或为空字符串
                - 格式不正确（不包含'年'）
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("minimum_work_time",
                                     ["最低工作年限不能为空字符串",
                                      f"Error format data: {value}"])
        # 简单验证工作年限格式，如"3年"
        if '年' not in value:
            raise JobValidationError("minimum_work_time",
                                     ["最低工作年限格式不正确，应包含'年'，如'3年'",
                                      f"Error format data: {value}"])
        self._data["minimum_work_time"] = value
        return self

    def with_job_sort(self, value: str) -> 'JobDataBuilder':
        """
        设置工作类型（全职/兼职等），必须是预定义类型之一

        Args:
            value: 工作类型字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError:
                - 若输入不是字符串类型或为空字符串
                - 不在预定义的有效工作类型列表中
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("job_sort",
                                     ["工作类型不能为空字符串",
                                      f"Error format data: {value}"])
        # 验证是否为常见的工作类型
        valid_types = ["全职", "兼职", "实习", "远程"]
        if value not in valid_types:
            raise JobValidationError("job_sort",
                                     [f"工作类型必须是以下之一: {', '.join(valid_types)}",
                                      f"Error format data: {value}"])
        self._data["job_sort"] = value
        return self

    def with_job_category(self, value: str) -> 'JobDataBuilder':
        """
        设置职位类别，必须是预定义类别之一

        Args:
            value: 职位类别字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若类别不在预定义列表中
        """
        if value not in _job_categories:
            raise JobValidationError('job_category',
                                     ["职位类别仅包含以下类别: [" +
                                      ', '.join(["'" + c + "'" for c in _job_categories]) + "]"])
        self._data['job_category'] = value
        return self

    def with_required_skills(self, value: List[str]) -> 'JobDataBuilder':
        """
        设置职位要求的技能列表

        Args:
            value: 技能字符串组成的列表

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是列表类型
        """
        if not isinstance(value, list):
            raise JobValidationError('required_skills',
                                     ["职位要求的技能必须是由字符串组成的列表",
                                      f"Error data: {value}"])
        self._data['required_skills'] = value
        return self

    def build(self) -> Dict[str, Any]:
        """
        构建并返回验证后的职位信息数据

        检查所有必要字段是否已填充，确保没有缺失的必填项

        Returns:
            包含完整职位信息的字典（数据副本，避免外部修改）

        Raises:
            JobValidationError: 若存在未填充的必要字段
        """
        # 验证所有字段是否已设置（没有None值）
        required_fields = self._data.keys()
        missing_fields = [field for field in required_fields if self._data[field] is None]
        if missing_fields:
            raise JobValidationError(
                "build",
                ["缺少必要字段: [" +
                 ', '.join(["'" + f + "'" for f in missing_fields]) + "]"])
        # 返回数据副本，防止外部意外修改内部数据
        return dict(self._data)

    @staticmethod
    def generate_random_data() -> 'JobDataBuilder':
        """
        生成随机模拟的职位信息数据

        使用Faker库生成符合格式要求的随机职位信息，包含所有必要字段，
        可直接用于测试或演示。

        Returns:
            已填充随机数据的JobDataBuilder实例
        """
        fake = Faker("zh_CN")  # 使用中文环境生成模拟数据
        builder = JobDataBuilder()

        # 职位名称池
        job_titles = [
            "软件工程师", "前端开发工程师", "后端开发工程师", "全栈工程师",
            "数据分析师", "产品经理", "UI/UX设计师", "测试工程师",
            "运维工程师", "DevOps工程师", "机器学习工程师", "算法工程师",
            "市场营销专员", "人力资源经理", "财务分析师"
        ]

        # 省份与对应城市映射
        province_cities = {
            "北京市": ["北京市"],
            "上海市": ["上海市"],
            "广东省": ["广州市", "深圳市", "珠海市", "佛山市"],
            "江苏省": ["南京市", "苏州市", "无锡市", "常州市"],
            "浙江省": ["杭州市", "宁波市", "温州市", "绍兴市"],
            "四川省": ["成都市", "绵阳市", "德阳市"],
            "山东省": ["济南市", "青岛市", "烟台市"],
            "湖北省": ["武汉市", "宜昌市", "襄阳市"],
            "河南省": ["郑州市", "洛阳市", "开封市"],
            "陕西省": ["西安市", "咸阳市"]
        }

        # 随机选择省份和对应城市
        province = random.choice(list(province_cities.keys()))
        city = random.choice(province_cities[province])
        country = '中国'
        category = random.choice(_job_categories)

        # 从对应类别中随机选择要求的技能
        required_skills = random.sample(_job_category_skills[category],
                                        random.randint(1, len(_job_category_skills[category])))

        # 薪资范围选项
        salary_ranges = [
            "5k-8k", "8k-12k", "12k-15k", "15k-20k",
            "20k-25k", "25k-30k", "30k-40k", "40k-50k"
        ]

        # 工作年限选项
        work_years = ["1年", "2年", "3年", "3-5年", "5-10年", "10年以上"]

        # 生成职位描述（3-5段）
        job_description = "\n\n".join([fake.paragraph(nb_sentences=3) for _ in range(random.randint(3, 5))])

        # 生成职位要求（ bullet points 形式）
        requirements_list = [
            f"• {fake.sentence()}",
            f"• {fake.sentence()}",
            f"• {fake.sentence()}",
            f"• {fake.sentence()}",
            f"• {fake.sentence()}"
        ]
        requirements = "\n".join(requirements_list[:random.randint(3, 5)])

        # 组装所有数据
        return (builder
                .with_job_id()
                .with_job_category(category)
                .with_required_skills(required_skills)
                .with_job_title(random.choice(job_titles))
                .with_company_name(fake.company())
                .with_country(country)
                .with_city(city)
                .with_province(province)
                .with_salary_range(random.choice(salary_ranges))
                .with_job_description(job_description)
                .with_requirements(requirements)
                .with_minimum_work_time(random.choice(work_years))
                .with_job_sort(random.choice(["全职", "兼职", "实习", "远程"])))


def main():
    """示例入口函数，生成并打印随机简历和职位数据"""
    from pprint import pprint
    pprint(ResumeDataBuilder.generate_random_data().build())
    pprint(JobDataBuilder.generate_random_data().build())

    # print(json.dumps(ResumeDataBuilder.generate_random_data().build()))


if __name__ == '__main__':
    main()
