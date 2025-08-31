from sqlalchemy import Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from pydantic import BaseModel, validator, field_validator
from typing import List, Optional, Dict
import datetime

# 基础模型
Base = declarative_base()

# 复用原代码中的职位类别定义（保持一致性）
job_categories = [
    "技术", "产品", "运营", "市场", "销售", "人力资源", "财务"
]


class Resume(Base):
    """简历数据模型，对应SQLAlchemy版本"""
    __tablename__ = "resumes"  # 数据库表名

    resume_id = Column(Integer, primary_key=True, unique=True, comment="简历ID")
    category = Column(String(20), nullable=False, comment="求职岗位类别")
    title = Column(String(200), nullable=False, comment="简历标题")
    desc = Column(Text, nullable=False, comment="简历描述（期望薪资/城市等）")
    photo = Column(String(255), nullable=True, comment="照片URL")  # 使用String存储URL
    email = Column(String(100), nullable=False, comment="联系邮箱")
    name = Column(String(50), nullable=False, comment="姓名")
    education = Column(JSON, nullable=False, comment="教育经历")  # 存储列表[{}]
    work_experience = Column(JSON, nullable=False, comment="工作经历")  # 存储列表[{}]
    project_experience = Column(JSON, nullable=False, comment="项目经历")  # 存储列表[{}]
    skill = Column(JSON, nullable=False, comment="技能列表")  # 存储列表[]
    introduction = Column(Text, nullable=False, comment="个人介绍")
    address = Column(Text, nullable=False, comment="联系地址")
    created_at = Column(DateTime, default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), comment="更新时间")

    def __repr__(self):
        return f"<Resume {self.name}的简历（ID: {self.resume_id}）>"


# Pydantic模型用于数据验证和序列化
class ResumeBase(BaseModel):
    resume_id: int
    category: str
    title: str
    desc: str
    photo: Optional[str] = None
    email: str
    name: str
    education: List[Dict]  # 教育经历是字典列表
    work_experience: List[Dict]  # 工作经历是字典列表
    project_experience: List[Dict]  # 项目经历是字典列表
    skill: List[str]  # 技能是字符串列表
    introduction: str
    address: str

    # 验证职位类别
    @field_validator('category')
    def category_must_be_valid(self, v):
        if v not in job_categories:
            raise ValueError(f'求职岗位类别必须是以下之一: {job_categories}')
        return v

    # 验证邮箱格式（Pydantic会自动验证EmailStr，但这里显式保留以便后续扩展）
    @field_validator('email')
    def email_must_be_valid(self, v):
        if '@' not in v:
            raise ValueError('邮箱格式不正确')
        return v


class ResumeCreate(ResumeBase):
    pass


class ResumeUpdate(BaseModel):
    category: Optional[str] = None
    title: Optional[str] = None
    desc: Optional[str] = None
    photo: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None
    education: Optional[List[Dict]] = None
    work_experience: Optional[List[Dict]] = None
    project_experience: Optional[List[Dict]] = None
    skill: Optional[List[str]] = None
    introduction: Optional[str] = None
    address: Optional[str] = None

    # 验证职位类别
    @field_validator('category')
    def category_must_be_valid(self, v):
        if v is not None and v not in job_categories:
            raise ValueError(f'求职岗位类别必须是以下之一: {job_categories}')
        return v

    # 验证邮箱格式
    @field_validator('email')
    def email_must_be_valid(self, v):
        if v is not None and '@' not in v:
            raise ValueError('邮箱格式不正确')
        return v


class ResumeInDB(ResumeBase):
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True
