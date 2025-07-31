from django.db import transaction
from django.shortcuts import render
from rest_framework import status
from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny

from .models import Resume, job_categories
from .serializers import ResumeSerializer
from function_api import *


class ResumeViewSet(viewsets.ModelViewSet):
    queryset = Resume.objects.all()
    serializer_class = ResumeSerializer


@api_view(['POST'])
@permission_classes([AllowAny])
def batch_upload_resume_data(request) -> Response:
    """
    批量上传简历数据接口
    支持批量创建新简历，自动处理重复ID和数据验证
    """
    # 验证请求数据格式
    if not request.data or not isinstance(request.data, list):
        return Response(
            {"error": "请求数据必须是 list 类型"},
            status=status.HTTP_400_BAD_REQUEST
        )
    req_data = request.data

    # 定义必填字段
    required_fields = [
        'resume_id', 'category', 'title', 'email', 'name'
    ]

    # 分离有效数据和无效数据
    valid_resumes = []
    invalid_entries = []
    existing_resume_ids = set(Resume.objects.values_list('resume_id', flat=True))

    for index, resume_data in enumerate(req_data):
        try:
            # 基础验证：检查必填字段
            for field in required_fields:
                if field not in resume_data:
                    raise ValueError(f"缺少必填字段: {field}")

            # 验证简历ID是否已存在
            if resume_data['resume_id'] in existing_resume_ids:
                raise ValueError(f"简历ID已存在: {resume_data['resume_id']}")

            # 验证职位类别
            if resume_data['category'] not in job_categories:
                raise ValueError(f"无效的职位类别: {resume_data['category']}, 允许的值: {job_categories}")

            # 处理JSON字段的默认值和格式验证
            json_fields = ['education', 'work_experience', 'project_experience', 'skill']
            for field in json_fields:
                # 设置默认值
                resume_data.setdefault(field, [])
                # 验证格式
                if not isinstance(resume_data[field], list):
                    raise ValueError(f"{field}必须是列表格式")

            # 处理其他可选字段默认值
            resume_data.setdefault('desc', '')
            resume_data.setdefault('photo', '')
            resume_data.setdefault('introduction', '')
            resume_data.setdefault('address', '')

            # 创建简历对象并添加到有效列表
            valid_resumes.append(Resume(**resume_data))
            existing_resume_ids.add(resume_data['resume_id'])  # 避免同批次内ID重复

        except Exception as e:
            invalid_entries.append({
                "index": index,
                "data": resume_data,
                "error": str(e)
            })

    # 批量创建有效简历
    created_count = 0
    if valid_resumes:
        try:
            with transaction.atomic():
                Resume.objects.bulk_create(valid_resumes)
                created_count = len(valid_resumes)
        except Exception as e:
            return Response(
                {"error": f"批量保存失败: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    # 返回处理结果（与职位接口格式保持一致）
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
def batch_get_resume_data(request) -> Response:
    """
    批量获取简历数据接口

    查询参数:
    - ids: 简历ID列表，用逗号分隔，如 ?ids=1,2,3,4
    - category: 按职位类别筛选，如 ?category=技术
    - page: 页码，默认为1
    - page_size: 每页数量，默认为10，最大100

    响应格式与批量上传接口保持一致风格
    """
    try:
        # 解析查询参数
        ids_param = request.query_params.getlist('ids', '')
        category = request.query_params.get('category', '')
        page = int(request.query_params.get('page', -1))
        page_size = int(request.query_params.get('page_size', -1))
        # 限制最大页大小
        page_size = min(page_size, 100)
        offset = (page - 1) * page_size

        # 基础查询集
        queryset = Resume.objects.all()

        # 按ID列表筛选
        if ids_param:
            try:
                queryset = queryset.filter(resume_id__in=ids_param)
            except ValueError:
                return Response(
                    {"error": "ids参数格式错误，必须是逗号分隔的整数"},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # 按职位类别筛选
        if category:
            queryset = queryset.filter(category=category)

        # 计算总数和分页
        total = queryset.count()
        resumes = queryset.order_by('resume_id')[offset:offset + page_size]\
            if page > 0 and page_size > 0\
            else queryset.order_by('resume_id')

        # 构建返回数据
        result = []
        for resume in resumes:
            result.append({
                "resume_id": resume.resume_id,
                "category": resume.category,
                "title": resume.title,
                "desc": resume.desc,
                "photo": resume.photo,
                "email": resume.email,
                "name": resume.name,
                "education": resume.education,
                "work_experience": resume.work_experience,
                "project_experience": resume.project_experience,
                "skill": resume.skill,
                "introduction": resume.introduction,
                "address": resume.address,
                "created_at": resume.created_at.isoformat() if resume.created_at else None,
                "updated_at": resume.updated_at.isoformat() if resume.updated_at else None
            })

        # 返回分页和数据
        return Response({
            "summary": {
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": ((total + page_size - 1) // page_size) if page > 0 and page_size > 0 else -1
            },
            "data": result
        })

    except Exception as e:
        return Response(
            {"error": f"获取数据失败: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
