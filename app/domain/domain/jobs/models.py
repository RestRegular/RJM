from sqlalchemy import Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, field_validator
from typing import List, Optional
import datetime

# 基础模型
Base = declarative_base()

job_categories = [
    "技术", "产品", "运营", "市场", "销售", "人力资源", "财务"
]
job_sorts = ["全职", "兼职", "实习", "远程"]


class Job(Base):
    """职位数据模型，对应SQLAlchemy版本"""
    __tablename__ = "jobs"  # 数据库表名

    job_id = Column(Integer, primary_key=True, unique=True, comment="职位ID")
    job_title = Column(String(200), nullable=False, comment="职位名称")
    company_name = Column(String(100), nullable=False, comment="公司名称")
    city = Column(String(50), nullable=False, comment="工作城市")
    province = Column(String(50), nullable=False, comment="所在省份")
    country = Column(String(50), nullable=False, comment="所在国家")
    salary_range = Column(String(20), nullable=False, comment="薪资范围（如18k-25k）")
    salary_low = Column(Integer, nullable=False, comment="薪资下限（k）")
    salary_high = Column(Integer, nullable=False, comment="薪资上限（k）")
    job_description = Column(Text, nullable=False, comment="职位描述")
    requirements = Column(Text, nullable=False, comment="职位要求")
    job_category = Column(String(20), nullable=False, comment="职位类别")
    minimum_work_time = Column(String(20), nullable=False, comment="最低工作年限（如3年）")
    job_sort = Column(String(10), nullable=False, comment="工作类型")
    required_skills = Column(JSON, nullable=False, comment="要求的技能列表")  # 存储列表[]
    created_at = Column(DateTime, default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), comment="更新时间")

    def __repr__(self):
        return f"<Job {self.job_title}（{self.company_name}）>"


# Pydantic模型用于数据验证和序列化
class JobBase(BaseModel):
    job_id: int
    job_title: str
    company_name: str
    city: str
    province: str
    country: str
    salary_range: str
    salary_low: int
    salary_high: int
    job_description: str
    requirements: str
    job_category: str
    minimum_work_time: str
    required_skills: List[str]
    job_sort: str

    # 验证薪资范围
    @field_validator('salary_low', 'salary_high')
    def salary_must_be_positive(self, v):
        if v < 0:
            raise ValueError('薪资必须为非负值')
        return v

    # 验证职位类别
    @field_validator('job_category')
    def category_must_be_valid(self, v):
        if v not in job_categories:
            raise ValueError(f'职位类别必须是以下之一: {job_categories}')
        return v

    # 验证工作类型
    @field_validator('job_sort')
    def sort_must_be_valid(self, v):
        if v not in job_sorts:
            raise ValueError(f'工作类型必须是以下之一: {job_sorts}')
        return v


class JobCreate(JobBase):
    pass


class JobUpdate(BaseModel):
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    city: Optional[str] = None
    province: Optional[str] = None
    country: Optional[str] = None
    salary_range: Optional[str] = None
    salary_low: Optional[int] = None
    salary_high: Optional[int] = None
    job_description: Optional[str] = None
    requirements: Optional[str] = None
    job_category: Optional[str] = None
    minimum_work_time: Optional[str] = None
    required_skills: Optional[List[str]] = None
    job_sort: Optional[str] = None

    # 同样添加验证器
    @field_validator('salary_low', 'salary_high')
    def salary_must_be_positive(self, v):
        if v is not None and v < 0:
            raise ValueError('薪资必须为非负值')
        return v

    @field_validator('job_category')
    def category_must_be_valid(self, v):
        if v is not None and v not in job_categories:
            raise ValueError(f'职位类别必须是以下之一: {job_categories}')
        return v

    @field_validator('job_sort')
    def sort_must_be_valid(self, v):
        if v is not None and v not in job_sorts:
            raise ValueError(f'工作类型必须是以下之一: {job_sorts}')
        return v


class JobInDB(JobBase):
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True
