from fastapi import APIRouter, HTTPException, Query, Body, Depends
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.domain.jobs.models import job_categories, job_sorts, Job as JobModel, JobCreate as DBJobCreate
from app.api.function_api import RJ_MATCHER
# 假设您有一个数据库会话依赖项
from app.database import get_db

router = APIRouter(tags=["jobs"])


# Pydantic模型定义
class JobBase(BaseModel):
    job_id: int
    job_title: str
    company_name: str
    city: str
    salary_low: float = Field(..., ge=0)
    salary_high: float = Field(..., ge=0)
    job_category: str
    job_sort: str
    province: str = ""
    country: str = "中国"
    job_description: str = ""
    requirements: str = ""
    minimum_work_time: str = "不限"
    required_skills: List[str] = []
    salary_range: Optional[str] = None  # 将由系统生成

    @classmethod
    @field_validator('salary_high')
    def salary_high_must_be_greater_than_low(cls, v, values):
        if 'salary_low' in values and v < values['salary_low']:
            raise ValueError('薪资上限不能小于薪资下限')
        return v

    @classmethod
    @field_validator('job_category')
    def validate_job_category(cls, v):
        if v not in job_categories:
            raise ValueError(f"无效的职位类别，允许的值: {job_categories}")
        return v

    @classmethod
    @field_validator('job_sort')
    def validate_job_sort(cls, v):
        if v not in job_sorts:
            raise ValueError(f"无效的工作类型，允许的值: {job_sorts}")
        return v


class JobCreate(JobBase):
    # 创建时不需要ID，由数据库生成或客户端提供
    pass


class JobResponse(JobBase):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BatchUploadResponse(BaseModel):
    summary: Dict[str, int] = Field(..., description="处理结果摘要")
    failed_entries: List[Dict[str, Any]] = Field(..., description="失败的条目及原因")


@router.post("/batch-upload", response_model=BatchUploadResponse,
             description="批量上传职位数据接口，支持批量创建新职位，自动处理重复ID和数据验证")
async def batch_upload_job_data(
        jobs: List[JobCreate] = Body(..., description="职位数据列表"),
        db: Session = Depends(get_db)
):
    # 分离有效数据和无效数据
    valid_jobs = []
    invalid_entries = []

    # 查询已存在的job_id
    existing_job_ids = [job.job_id for job in db.query(JobModel.job_id).all()]
    existing_job_ids = set(existing_job_ids)

    for index, job_data in enumerate(jobs):
        try:
            # 检查职位ID是否已存在
            if job_data.job_id in existing_job_ids:
                raise ValueError(f"职位ID已存在: {job_data.job_id}")

            # 生成薪资范围字符串
            salary_range = f"{job_data.salary_low}k-{job_data.salary_high}k"

            # 创建Job模型实例
            job_instance = JobModel(
                **job_data.model_dump(exclude={"salary_range"}),
                salary_range=salary_range
            )

            valid_jobs.append(job_instance)
            existing_job_ids.add(job_data.job_id)  # 避免同批次内ID重复

        except Exception as e:
            invalid_entries.append({
                "index": index,
                "data": job_data.model_dump(),
                "error": str(e)
            })

    # 批量创建有效职位
    created_count = 0
    if valid_jobs:
        db.add_all(valid_jobs)
        db.commit()
        created_count = len(valid_jobs)

    # 返回处理结果
    response = {
        "summary": {
            "total_received": len(jobs),
            "created": created_count,
            "failed": len(invalid_entries)
        },
        "failed_entries": invalid_entries
    }

    # 根据是否有失败项返回不同的状态码
    if invalid_entries:
        return response
    return response


class MultiDimensionSearchResponse(BaseModel):
    count: int
    results: List[Dict[str, Any]]


@router.get("/multi-dimension-index", response_model=MultiDimensionSearchResponse,
            description="多维度索引岗位接口，支持多条件组合筛选职位")
async def multi_dimension_index_jobs(
        category: Optional[str] = Query(None, description="职位分类（如'技术开发'）"),
        location: Optional[str] = Query(None, description="工作地点（如'北京'）"),
        skills: Optional[str] = Query(None, description="必需技能列表（逗号分隔，如 'python,java'）"),
        salary_min: Optional[float] = Query(None, description="最低薪资"),
        salary_max: Optional[float] = Query(None, description="最高薪资"),
        experience_min: Optional[float] = Query(None, description="最低工作经验（年）"),
        experience_max: Optional[float] = Query(None, description="最高工作经验（年）"),
        limit: int = Query(100, description="最大返回数量", le=1000),
        db: Session = Depends(get_db)
):
    try:
        # 处理技能列表
        skills_list = [s.strip() for s in skills.split(',')] if skills else None

        # 调用多维度搜索逻辑
        jobs = RJ_MATCHER.search_jobs(
            category=category,
            location=location,
            skills=skills_list,
            salary_min=salary_min,
            salary_max=salary_max,
            experience_min=experience_min,
            experience_max=experience_max,
            limit=limit,
            db=db  # 传递数据库会话
        )

        # 序列化结果并添加匹配分数
        serialized_jobs = []
        for job in jobs:
            # 假设job是ORM对象，转换为字典
            job_dict = JobResponse.model_validate(job).model_dump()
            job_dict['match_score'] = getattr(job, 'match_score', 0)  # 添加匹配分数
            serialized_jobs.append(job_dict)

        return {
            'count': len(serialized_jobs),
            'results': serialized_jobs
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"多维度索引岗位失败: {str(e)}"
        )


class BatchGetResponse(BaseModel):
    summary: Dict[str, Union[int, str]]
    data: List[JobResponse]


@router.get("/batch-get", response_model=BatchGetResponse,
            description="批量获取岗位数据接口，支持多条件筛选和分页")
async def batch_get_job_data(
        ids: Optional[str] = Query(None, description="岗位ID列表，用逗号分隔，如'1001,1002,1003'"),
        category: Optional[str] = Query(None, description="按职位类别筛选"),
        location: Optional[str] = Query(None, description="按工作地点筛选"),
        page: int = Query(1, description="页码，默认为1"),
        page_size: int = Query(10, description="每页数量，默认为10，最大100", le=100),
        db: Session = Depends(get_db)
):
    try:
        # 计算分页偏移量
        offset = (page - 1) * page_size if page > 0 else 0

        # 基础查询集
        query = db.query(JobModel)

        # 按ID列表筛选
        if ids:
            try:
                job_ids = [int(id_.strip()) for id_ in ids.split(',') if id_.strip()]
                query = query.filter(JobModel.job_id.in_(job_ids))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="ids参数格式错误，必须是逗号分隔的有效ID"
                )

        # 按职位类别筛选
        if category:
            query = query.filter(JobModel.job_category == category)

        # 按工作地点筛选
        if location:
            query = query.filter(JobModel.city == location)

        # 计算总数
        total = query.count()

        # 处理分页
        if page > 0 and page_size > 0:
            jobs = query.order_by(JobModel.job_id).offset(offset).limit(page_size).all()
        else:
            jobs = query.order_by(JobModel.job_id).all()

        # 计算总页数
        if page > 0 and page_size > 0:
            total_pages = (total + page_size - 1) // page_size
        else:
            total_pages = -1  # 表示不分页

        # 构建响应
        return {
            "summary": {
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": total_pages
            },
            "data": [JobResponse.model_validate(job) for job in jobs]
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"获取岗位数据失败: {str(e)}"
        )


# 职位视图集的CRUD操作
@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(JobModel).filter(JobModel.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="职位不存在")
    return JobResponse.model_validate(job)


@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(job: JobCreate, db: Session = Depends(get_db)):
    if db.query(JobModel).filter(JobModel.job_id == job.job_id).first():
        raise HTTPException(status_code=400, detail=f"职位ID已存在: {job.job_id}")

    # 生成薪资范围
    salary_range = f"{job.salary_low}k-{job.salary_high}k"

    job_instance = JobModel(
        **job.model_dump(exclude={"salary_range"}),
        salary_range=salary_range
    )
    db.add(job_instance)
    db.commit()
    db.refresh(job_instance)  # 刷新以获取数据库生成的字段
    return JobResponse.model_validate(job_instance)


@router.put("/{job_id}", response_model=JobResponse)
async def update_job(job_id: int, job: JobCreate, db: Session = Depends(get_db)):
    job_instance = db.query(JobModel).filter(JobModel.job_id == job_id).first()
    if not job_instance:
        raise HTTPException(status_code=404, detail="职位不存在")

    # 确保更新时不会更改job_id
    if job.job_id != job_id:
        raise HTTPException(status_code=400, detail="不能修改职位ID")

    # 更新字段
    for key, value in job.model_dump().items():
        if key != "salary_range":  # 不直接更新薪资范围
            setattr(job_instance, key, value)

    # 更新薪资范围
    job_instance.salary_range = f"{job.salary_low}k-{job.salary_high}k"
    db.commit()
    db.refresh(job_instance)
    return JobResponse.model_validate(job_instance)


@router.delete("/{job_id}", status_code=204)
async def delete_job(job_id: int, db: Session = Depends(get_db)):
    job_instance = db.query(JobModel).filter(JobModel.job_id == job_id).first()
    if not job_instance:
        raise HTTPException(status_code=404, detail="职位不存在")

    db.delete(job_instance)
    db.commit()
    return None