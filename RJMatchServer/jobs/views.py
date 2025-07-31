import json

from django.shortcuts import render
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny

from .models import Job
from .serializers import JobSerializer, job_categories, job_sorts

from function_api import *


class JobViewSet(viewsets.ModelViewSet):
    queryset = Job.objects.all()
    serializer_class = JobSerializer


@api_view(['POST'])
@permission_classes([AllowAny])
def batch_upload_job_data(request) -> Response:
    """
    批量上传职位数据接口
    支持批量创建新职位，自动处理重复ID和数据验证
    """
    # 验证请求数据格式
    if not request.data or not isinstance(request.data, list):
        return Response(
            {"error": "请求数据必须是 list 类型"},
            status=status.HTTP_400_BAD_REQUEST
        )
    req_data = request.data

    required_fields = [
        'job_id', 'job_title', 'company_name', 'city',
        'salary_low', 'salary_high', 'job_category', 'job_sort'
    ]

    # 分离有效数据和无效数据
    valid_jobs = []
    invalid_entries = []
    existing_job_ids = set(Job.objects.values_list('job_id', flat=True))

    for index, job_data in enumerate(req_data):
        try:
            # 基础验证
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

            valid_jobs.append(Job(**job_data))
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
        Job.objects.bulk_create(valid_jobs)
        created_count = len(valid_jobs)

    # 返回处理结果
    return Response({
        "summary": {
            "total_received": len(req_data),
            "created": created_count,
            "failed": len(invalid_entries)
        },
        "failed_entries": invalid_entries
    }, status=status.HTTP_200_OK if invalid_entries else status.HTTP_201_CREATED)


@api_view(['GET'])
@permission_classes([AllowAny])
def multi_dimension_index_jobs(request) -> Response:
    """
    多维度索引岗位接口
    支持多条件组合筛选职位，基于多维度索引高效查询并返回按匹配度排序的结果

    查询参数:
        category: 职位分类（如"技术开发"）
        location: 工作地点（如"北京"）
        skills: 必需技能列表（逗号分隔，如"python,java"）
        salary_min: 最低薪资（数字）
        salary_max: 最高薪资（数字）
        experience_min: 最低工作经验（年）
        experience_max: 最高工作经验（年）
        limit: 最大返回数量（默认100）

    返回:
        按匹配度降序排列的职位列表，包含match_score字段
    """
    try:
        # 解析查询参数
        params = request.query_params
        category = params.get('category')
        location = params.get('location')

        # 处理技能列表（逗号分隔转列表）
        skills_str = params.get('skills')
        skills = [s.strip() for s in skills_str.split(',')] if skills_str else None

        # 处理数字类型参数
        def parse_number(param_name):
            val = params.get(param_name)
            return float(val) if val and val.replace('.', '', 1).isdigit() else None

        salary_min = parse_number('salary_min')
        salary_max = parse_number('salary_max')
        experience_min = parse_number('experience_min')
        experience_max = parse_number('experience_max')
        limit = int(params.get('limit', 100))

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

        # 序列化结果（保留match_score字段）
        serialized_jobs = JobSerializer(jobs, many=True).data
        for i, job in enumerate(serialized_jobs):
            job['match_score'] = jobs[i]['match_score']

        return Response({
            'count': len(serialized_jobs),
            'results': serialized_jobs
        })

    except Exception as e:
        return Response(
            {'error': f"多维度索引岗位失败: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
@permission_classes([AllowAny])
def batch_get_job_data(request) -> Response:
    """
    批量获取岗位数据接口

    查询参数:
    - ids: 岗位ID列表，用逗号分隔，如 ?ids=1001,1002,1003
    - category: 按职位类别筛选，如 ?category=技术开发
    - location: 按工作地点筛选，如 ?location=北京
    - page: 页码，默认为1
    - page_size: 每页数量，默认为10，最大100

    响应格式与批量获取简历数据接口保持一致风格
    """
    try:
        # 解析查询参数
        ids_str = request.query_params.get('ids', '')
        category = request.query_params.get('category', '')
        location = request.query_params.get('location', '')
        page = int(request.query_params.get('page', 1))
        page_size = int(request.query_params.get('page_size', 10))

        # 限制最大页大小
        page_size = min(page_size, 100)
        # 计算分页偏移量（处理page为0或负数的异常情况）
        offset = (page - 1) * page_size if page > 0 else 0

        # 基础查询集
        queryset = Job.objects.all()

        # 按ID列表筛选（支持逗号分隔的多个ID）
        if ids_str:
            try:
                job_ids = [id.strip() for id in ids_str.split(',') if id.strip()]
                queryset = queryset.filter(job_id__in=job_ids)
            except ValueError:
                return Response(
                    {"error": "ids参数格式错误，必须是逗号分隔的有效ID"},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # 按职位类别筛选
        if category:
            queryset = queryset.filter(job_category=category)

        # 按工作地点筛选
        if location:
            queryset = queryset.filter(city=location)

        # 计算总数和分页数据
        total = queryset.count()
        # 处理分页逻辑（page<=0时返回全部数据）
        jobs = queryset.order_by('job_id')[offset:offset + page_size] \
            if page > 0 and page_size > 0 \
            else queryset.order_by('job_id')

        # 构建返回数据列表
        result = []
        for job in jobs:
            # 序列化职位数据（与简历接口字段风格保持一致）
            result.append({
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
            })

        # 计算总页数（page<=0时返回-1表示不分页）
        total_pages = ((total + page_size - 1) // page_size) if page > 0 and page_size > 0 else -1

        # 返回分页信息和数据
        return Response({
            "summary": {
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": total_pages
            },
            "data": result
        })

    except Exception as e:
        return Response(
            {"error": f"获取岗位数据失败: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
