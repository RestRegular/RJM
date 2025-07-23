#
# Created by RestRegular on 2025/7/23
#
from typing import List, Dict, Any, Optional, Union

from src.components.error_manager import ResumeValidationError, JobValidationError

__all__ = [
    'ResumeDataBuilder',
    'JobDataBuilder'
]


def _convert_camel_case_to_underscore_case(name: str) -> str:
    """
    将驼峰式命名转换为下划线命名

    用于将字典中的键名（通常为驼峰式）转换为构建器类中with_xxx方法的命名格式
    例如: "workExperience" -> "work_experience"，最终对应with_work_experience方法

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
    """

    def __init__(self):
        """初始化简历数据结构

        定义简历包含的所有字段及其初始值，None表示待填充，列表字段初始化为空列表
        """
        self._data: Dict[str, Union[str, List, None]] = {
            "title": None,  # 简历标题
            "desc": None,  # 简历描述（期望薪资、工作城市等）
            "photo": None,  # 照片URL
            "email": None,  # 联系邮箱
            "name": None,  # 姓名
            "education": [],  # 教育经历列表
            "workExperience": [],  # 工作经历列表
            "projectExperience": [],  # 项目经历列表
            "skill": [],  # 技能列表
            "introduction": None,  # 个人介绍
            "address": None  # 联系地址
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ResumeDataBuilder':
        """
        从字典数据构建ResumeDataBuilder对象

        遍历字典中的键值对，自动调用对应的with_xxx方法进行数据填充
        仅处理存在对应with_xxx方法的键值对

        Args:
            data: 包含简历信息的字典，键应为驼峰式命名（如"workExperience"）

        Returns:
            初始化后的ResumeDataBuilder实例
        """
        builder = cls()
        for key, value in data.items():
            # 转换键名为下划线格式并拼接with_前缀，得到方法名
            method_name = 'with_' + _convert_camel_case_to_underscore_case(key)
            # 检查方法是否存在且可调用
            if hasattr(builder, method_name) and callable(getattr(builder, method_name)):
                # 调用对应的with_xxx方法设置值
                getattr(builder, method_name)(value)
        return builder

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
        设置联系邮箱

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
        设置个人姓名

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
            raise ResumeValidationError("workExperience", "工作经历必须是列表类型")

        # 工作经历必须包含的字段
        required_fields = ["company", "job", "desc", "time"]
        for item in value:
            if not isinstance(item, dict):
                raise ResumeValidationError("workExperience", "工作经历项必须是字典类型")
            for field in required_fields:
                if field not in item:
                    raise ResumeValidationError("workExperience", f"工作经历缺少必要字段: {field}")

        self._data["workExperience"] = value
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
            raise ResumeValidationError("projectExperience", "项目经历必须是列表类型")

        # 项目经历必须包含的字段
        required_fields = ["title", "desc", "time", "role"]
        for item in value:
            if not isinstance(item, dict):
                raise ResumeValidationError("projectExperience", "项目经历项必须是字典类型")
            for field in required_fields:
                if field not in item:
                    raise ResumeValidationError("projectExperience", f"项目经历缺少必要字段: {field}")

        self._data["projectExperience"] = value
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


class JobDataBuilder:
    """
    职位信息构建器类

    提供链式调用的API用于构建和验证职位信息，确保数据格式正确且完整。
    支持从字典初始化数据，并通过build()方法获取最终验证后的职位数据。
    """

    def __init__(self):
        """初始化职位信息数据结构

        定义职位包含的所有字段及其初始值（均为None，表示待填充）
        """
        self._data: Dict[str, Optional[str]] = {
            "jobTitle": None,  # 职位名称
            "companyName": None,  # 公司名称
            "city": None,  # 工作城市
            "province": None,  # 所在省份
            "salaryRange": None,  # 薪资范围
            "jobDescription": None,  # 职位描述
            "requirements": None,  # 职位要求
            "minimumWorkTime": None,  # 最低工作年限
            "job_sort": None  # 工作类型（全职/兼职等）
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobDataBuilder':
        """
        从字典数据构建JobDataBuilder对象

        遍历字典中的键值对，自动调用对应的with_xxx方法进行数据填充
        仅处理存在对应with_xxx方法的键值对

        Args:
            data: 包含职位信息的字典，键应为驼峰式命名（如"jobTitle"）

        Returns:
            初始化后的JobDataBuilder实例
        """
        builder = cls()
        # 遍历字典中的键值对，调用对应的with_xxx方法
        for key, value in data.items():
            # 转换键名为下划线格式并拼接with_前缀，得到方法名
            method_name = 'with_' + _convert_camel_case_to_underscore_case(key)
            # 检查方法是否存在且可调用
            if hasattr(builder, method_name) and callable(getattr(builder, method_name)):
                # 调用对应的with_xxx方法设置值
                getattr(builder, method_name)(value)
        return builder

    def with_job_title(self, value: str) -> 'JobDataBuilder':
        """
        设置职位名称

        Args:
            value: 职位名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("jobTitle", "职位名称不能为空字符串")
        self._data["jobTitle"] = value
        return self

    def with_company_name(self, value: str) -> 'JobDataBuilder':
        """
        设置公司名称

        Args:
            value: 公司名称字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("companyName", "公司名称不能为空字符串")
        self._data["companyName"] = value
        return self

    def with_city(self, value: str) -> 'JobDataBuilder':
        """
        设置工作城市

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
        设置所在省份

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
        设置薪资范围

        Args:
            value: 薪资范围字符串，格式应为"xxk-yyk"（如"18k-25k"）

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError:
                - 若输入不是字符串类型或为空字符串
                - 格式不正确（不包含'-'）
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("salaryRange", "薪资范围不能为空字符串")
        # 简单验证薪资格式，如"18k-25k"
        if '-' not in value:
            raise JobValidationError("salaryRange", "薪资范围格式不正确，应包含'-'，如'18k-25k'")
        self._data["salaryRange"] = value
        return self

    def with_job_description(self, value: str) -> 'JobDataBuilder':
        """
        设置职位描述

        Args:
            value: 职位描述字符串

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError: 若输入不是字符串类型或为空字符串
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("jobDescription", "职位描述不能为空字符串")
        self._data["jobDescription"] = value
        return self

    def with_requirements(self, value: str) -> 'JobDataBuilder':
        """
        设置职位要求

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
        设置最低工作年限

        Args:
            value: 最低工作年限字符串，格式应为"x年"（如"3年"）

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError:
                - 若输入不是字符串类型或为空字符串
                - 格式不正确（不包含'年'）
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("minimumWorkTime", "最低工作年限不能为空字符串")
        # 简单验证工作年限格式，如"3年"
        if '年' not in value:
            raise JobValidationError("minimumWorkTime", "最低工作年限格式不正确，应包含'年'，如'3年'")
        self._data["minimumWorkTime"] = value
        return self

    def with_job_sort(self, value: str) -> 'JobDataBuilder':
        """
        设置工作类型（全职/兼职等）

        Args:
            value: 工作类型字符串，必须是预定义的有效值之一

        Returns:
            自身实例，支持链式调用

        Raises:
            JobValidationError:
                - 若输入不是字符串类型或为空字符串
                - 不在预定义的有效工作类型列表中
        """
        if not isinstance(value, str) or len(value.strip()) == 0:
            raise JobValidationError("job_sort", "工作类型不能为空字符串")
        # 验证是否为常见的工作类型
        valid_types = ["全职", "兼职", "实习", "远程"]
        if value not in valid_types:
            raise JobValidationError("job_sort", f"工作类型必须是以下之一: {', '.join(valid_types)}")
        self._data["job_sort"] = value
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
                ["缺少必要字段: [" + ', '.join(["'" + f + "'" for f in missing_fields]) + "]"])
        # 返回数据副本，防止外部意外修改内部数据
        return dict(self._data)
