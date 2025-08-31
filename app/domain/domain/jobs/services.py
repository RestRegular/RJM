from typing import List, Dict, Any, Optional, Tuple
from app.domain.jobs.models import job_categories, job_sorts, Job as JobModel


class JobService:
    @staticmethod
    def batch_upload_jobs(jobs_data: List[Dict[str, Any]]) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
        """
        批量上传职位数据处理

        参数:
            jobs_data: 职位数据列表

        返回:
            处理结果摘要和失败条目列表
        """
        valid_jobs = []
        invalid_entries = []
        existing_job_ids = set(JobModel.objects.values_list('job_id', flat=True))

        for index, job_data in enumerate(jobs_data):
            try:
                # 验证必填字段
                required_fields = [
                    'job_id', 'job_title', 'company_name', 'city',
                    'salary_low', 'salary_high', 'job_category', 'job_sort'
                ]
                for field in required_fields:
                    if field not in job_data:
                        raise ValueError(f"缺少必填字段: {field}")

                # 验证职位ID是否已存在
                if job_data['job_id'] in existing_job_ids:
                    raise ValueError(f"职位ID已存在: {job_data['job_id']}")

                # 验证薪资范围
                if job_data['salary_low'] < 0 or job_data['salary_high'] < 0:
                    raise ValueError("薪资不能为负数")
                if job_data['salary_low'] > job_data['salary_high']:
                    raise ValueError("薪资下限不能大于上限")

                # 自动生成薪资范围字符串
                job_data['salary_range'] = f"{job_data['salary_low']}k-{job_data['salary_high']}k"

                # 验证职位类别
                if job_data['job_category'] not in job_categories:
                    raise ValueError(f"无效的职位类别: {job_data['job_category']}, 允许的值: {job_categories}")

                # 验证工作类型
                if job_data['job_sort'] not in job_sorts:
                    raise ValueError(f"无效的工作类型: {job_data['job_sort']}, 允许的值: {job_sorts}")

                # 处理可选字段默认值
                job_data.setdefault('province', '')
                job_data.setdefault('country', '中国')
                job_data.setdefault('job_description', '')
                job_data.setdefault('requirements', '')
                job_data.setdefault('minimum_work_time', '不限')
                job_data.setdefault('required_skills', [])

                # 验证技能列表格式
                if not isinstance(job_data['required_skills'], list):
                    raise ValueError("required_skills必须是列表格式")

                valid_jobs.append(JobModel(**job_data))
                existing_job_ids.add(job_data['job_id'])  # 避免同批次内ID重复

            except Exception as e:
                invalid_entries.append({
                    "index": index,
                    "data": job_data,
                    "error": str(e)
                })

        # 批量创建有效职位
        created_count = 0
        if valid_jobs:
            JobModel.objects.bulk_create(valid_jobs)
            created_count = len(valid_jobs)

        # 准备返回结果
        summary = {
            "total_received": len(jobs_data),
            "created": created_count,
            "failed": len(invalid_entries)
        }

        return summary, invalid_entries

    @staticmethod
    def search_jobs(
            category: Optional[str] = None,
            location: Optional[str] = None,
            skills: Optional[List[str]] = None,
            salary_min: Optional[float] = None,
            salary_max: Optional[float] = None,
            experience_min: Optional[float] = None,
            experience_max: Optional[float] = None,
            limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        多维度搜索职位

        参数:
            各类搜索条件

        返回:
            带匹配分数的职位列表
        """
        from function_api import RJ_MATCHER  # 延迟导入，避免循环依赖

        # 调用多维度搜索逻辑
        jobs = RJ_MATCHER.search_jobs(
            category=category,
            location=location,
            skills=skills,
            salary_min=salary_min,
            salary_max=salary_max,
            experience_min=experience_min,
            experience_max=experience_max,
            limit=limit
        )

        # 处理结果，添加匹配分数
        result = []
        for job in jobs:
            job_dict = job.__dict__.copy()  # 假设job是ORM对象
            # 清理内部属性
            if '_state' in job_dict:
                del job_dict['_state']
            # 添加匹配分数
            job_dict['match_score'] = job.match_score
            result.append(job_dict)

        return result

    @staticmethod
    def batch_get_jobs(
            ids: Optional[List[str]] = None,
            category: Optional[str] = None,
            location: Optional[str] = None,
            page: int = 1,
            page_size: int = 10
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        批量获取职位数据

        参数:
            各类筛选条件和分页参数

        返回:
            分页摘要和职位数据列表
        """
        # 基础查询集
        queryset = JobModel.objects.all()

        # 按ID列表筛选
        if ids:
            queryset = queryset.filter(job_id__in=ids)

        # 按职位类别筛选
        if category:
            queryset = queryset.filter(job_category=category)

        # 按工作地点筛选
        if location:
            queryset = queryset.filter(city=location)

        # 计算总数
        total = queryset.count()

        # 处理分页
        offset = (page - 1) * page_size if page > 0 else 0
        if page > 0 and page_size > 0:
            jobs = queryset.order_by('job_id')[offset:offset + page_size]
        else:
            jobs = queryset.order_by('job_id')

        # 计算总页数
        if page > 0 and page_size > 0:
            total_pages = (total + page_size - 1) // page_size
        else:
            total_pages = -1  # 表示不分页

        # 构建数据列表
        data = []
        for job in jobs:
            job_dict = {
                "job_id": job.job_id,
                "job_title": job.job_title,
                "company_name": job.company_name,
                "job_category": job.job_category,
                "job_sort": job.job_sort,
                "city": job.city,
                "province": job.province,
                "country": job.country,
                "salary_low": job.salary_low,
                "salary_high": job.salary_high,
                "salary_range": job.salary_range,
                "job_description": job.job_description,
                "requirements": job.requirements,
                "minimum_work_time": job.minimum_work_time,
                "required_skills": job.required_skills,
                "created_at": job.created_at.isoformat() if hasattr(job, 'created_at') else None,
                "updated_at": job.updated_at.isoformat() if hasattr(job, 'updated_at') else None
            }
            data.append(job_dict)

        # 构建摘要信息
        summary = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": total_pages
        }

        return summary, data

    @staticmethod
    def get_job_by_id(job_id: str) -> Optional[JobModel]:
        """根据ID获取职位"""
        return JobModel.objects.filter(job_id=job_id).first()

    @staticmethod
    def create_job(job_data: Dict[str, Any]) -> JobModel:
        """创建新职位"""
        # 生成薪资范围
        salary_range = f"{job_data['salary_low']}k-{job_data['salary_high']}k"
        job_instance = JobModel(**job_data, salary_range=salary_range)
        job_instance.save()
        return job_instance

    @staticmethod
    def update_job(job_id: str, job_data: Dict[str, Any]) -> Optional[JobModel]:
        """更新职位信息"""
        job_instance = JobModel.objects.filter(job_id=job_id).first()
        if not job_instance:
            return None

        # 更新字段
        for key, value in job_data.items():
            if key != 'job_id':  # 不允许更新job_id
                setattr(job_instance, key, value)

        # 更新薪资范围
        job_instance.salary_range = f"{job_data['salary_low']}k-{job_data['salary_high']}k"
        job_instance.save()

        return job_instance

    @staticmethod
    def delete_job(job_id: str) -> bool:
        """删除职位"""
        deleted_count, _ = JobModel.objects.filter(job_id=job_id).delete()
        return deleted_count > 0
