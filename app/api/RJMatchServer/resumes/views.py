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


@api_view(['GET'])
@permission_classes([AllowAny])
def multi_dimension_index_resumes(request) -> Response:
    """
    多维度索引简历接口
    支持多条件组合筛选简历，基于多维度索引高效查询并返回按匹配度排序的结果

    查询参数:
        category: 求职岗位类别（如"技术开发"）
        location: 所在地区（如"北京"）
        skills: 技能列表（逗号分隔，如"python,java"）
        experience_min: 最低工作年限（年）
        experience_max: 最高工作年限（年）
        education: 最高学历（如"本科"）
        name: 姓名（模糊匹配）
        limit: 最大返回数量（默认100）
        page: 页码（默认1）
        page_size: 每页数量（默认20，最大100）

    返回:
        按匹配度降序排列的简历列表，包含match_score字段及分页信息
    """
    try:
        # 解析查询参数
        params = request.query_params
        category = params.get('category', '').strip()
        location = params.get('location', '').strip()
        education = params.get('education', '').strip()
        name = params.get('name', '').strip()

        # 处理技能列表（逗号分隔转列表）
        skills_str = params.get('skills', '').strip()
        skills = [s.strip() for s in skills_str.split(',')] if skills_str else None
        # 过滤空字符串
        if skills:
            skills = [s for s in skills if s]
            if not skills:
                skills = None

        # 处理数字类型参数
        def parse_number(param_name):
            val = params.get(param_name, '').strip()
            return float(val) if val and val.replace('.', '', 1).isdigit() else None

        experience_min = parse_number('experience_min')
        experience_max = parse_number('experience_max')

        # 处理分页参数
        try:
            page = int(params.get('page', 1))
            page = max(page, 1)  # 确保页码不小于1
        except ValueError:
            page = 1

        try:
            page_size = int(params.get('page_size', 20))
            page_size = max(1, min(page_size, 100))  # 限制每页数量在1-100之间
        except ValueError:
            page_size = 20

        # 计算偏移量和限制
        offset = (page - 1) * page_size
        # 为了分页准确，查询时获取足够多的数据
        limit = offset + page_size
        # 限制最大查询数量，防止性能问题
        limit = min(limit, 1000)

        # 调用多维度搜索逻辑
        resumes = RJ_MATCHER.search_resumes(
            category=category if category else None,
            location=location if location else None,
            skills=skills,
            experience_min=experience_min,
            experience_max=experience_max,
            education=education if education else None,
            name=name if name else None,
            limit=limit
        )

        # 计算总数量（用于分页信息）
        total = len(resumes)

        # 应用分页
        paginated_resumes = resumes[offset:offset + page_size]

        # 序列化结果（保留match_score字段）
        serialized_resumes = ResumeSerializer(paginated_resumes, many=True).data
        for i, resume in enumerate(serialized_resumes):
            resume['match_score'] = paginated_resumes[i]['match_score']

        # 计算总页数
        total_pages = (total + page_size - 1) // page_size

        return Response({
            'count': total,
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
            'has_next': page < total_pages,
            'has_previous': page > 1,
            'results': serialized_resumes
        })

    except Exception as e:
        return Response(
            {'error': f"多维度索引简历失败: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
