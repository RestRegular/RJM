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
