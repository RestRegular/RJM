from typing import Union, List, Dict, Any

from django.shortcuts import render
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny

from function_api import *
from resumes.models import Resume
from jobs.models import Job
from resumes.serializers import ResumeSerializer
from jobs.serializers import JobSerializer
from function_api.components.flink_matching_processor import ResumeMatcher, JobMatcher, set_match_threshold
from function_api.components.match_score_calculator import MatchDimensionWeightConfigBuilder, RJMatchDegreeCalculator
from function_api.components.data_manager import ResumeDataBuilder, JobDataBuilder

# 允许的匹配维度列表
ALLOWED_MATCH_DIMENSIONS = [
    "job_title_match",
    "skill_match",
    "work_experience_match",
    "location_match",
    "salary_match",
    "education_match",
    "project_experience_match",
    "other_factors"
]


def parse_ids(param: Union[str, List[str]]) -> Union[List[int], None]:
    """解析ID参数，支持单个ID或多个ID"""
    try:
        # 如果是列表
        if isinstance(param, list):
            return [int(id_str) for id_str in param]
        # 如果是单个值
        return [int(param)]
    except (ValueError, TypeError):
        return None


@api_view(['POST'])
@permission_classes([AllowAny])
def calculate_match_degree(request) -> Response:
    """计算指定简历与岗位的匹配度"""
    # 解析简历ID
    resume_id_param = request.data.get('resume_id')
    if not resume_id_param:
        return Response(
            {
                'status': 'failed',
                'message': 'resume_id参数是必需的'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    resume_ids = parse_ids(resume_id_param)
    if resume_ids is None:
        return Response(
            {
                'status': 'failed',
                'message': 'resume_id必须是整数或整数列表'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    # 解析岗位ID
    job_id_param = request.data.get('job_id')
    if not job_id_param:
        return Response(
            {
                'status': 'failed',
                'message': 'job_id参数是必需的'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    job_ids = parse_ids(job_id_param)
    if job_ids is None:
        return Response(
            {
                'status': 'failed',
                'message': 'job_id必须是整数或整数列表'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    # 解析匹配维度权重
    match_weights = {}
    has_weight_params = False
    match_weight_param = request.data.get('match_dimension_weight', {})
    for dimension in ALLOWED_MATCH_DIMENSIONS:
        weight_str = match_weight_param.get(dimension, None)
        if weight_str is not None:
            has_weight_params = True
            try:
                weight = float(weight_str)
                if weight < 0 or weight > 1:
                    return Response(
                        {
                            'status': 'failed',
                            'message': f"{dimension}权重必须在0到1之间"
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )
                match_weights[dimension] = weight
            except ValueError:
                return Response(
                    {
                        'status': 'failed',
                        'message': f"{dimension}权重必须是数字"
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

    # 如果提供了任何权重参数，则必须提供所有权重参数
    if has_weight_params:
        for dimension in ALLOWED_MATCH_DIMENSIONS:
            if dimension not in match_weights:
                return Response(
                    {
                        'status': 'failed',
                        'message': f"缺少{dimension}权重参数"
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

        # 检查权重之和是否为1（考虑浮点数精度问题）
        total_weight = sum(match_weights.values())
        if not abs(total_weight - 1.0) < 1e-9:
            return Response(
                {
                    'status': 'failed',
                    'message': f"所有维度权重之和必须等于1，当前总和为{round(total_weight, 2)}"
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        match_weights = MatchDimensionWeightConfigBuilder().from_dict(match_weights)
    else:
        # 使用默认权重
        match_weights = MatchDimensionWeightConfigBuilder.with_default_config()

    # 后续计算操作
    resumes = Resume.objects.filter(resume_id__in=resume_ids)
    resumes = {resume['resume_id']: ResumeDataBuilder.from_dict(resume) for resume in
               ResumeSerializer(resumes, many=True).data}
    jobs = Job.objects.filter(job_id__in=job_ids)
    jobs = {job['job_id']: JobDataBuilder.from_dict(job) for job in JobSerializer(jobs, many=True).data}
    result: Dict[int, any] = {int(rid): {} for rid in resume_ids}
    calculator = RJMatchDegreeCalculator(match_weights)
    for rid, resume_builder in resumes.items():
        for jid, job_builder in jobs.items():
            match_degree, _ = (calculator.calculate_overall_match(resume_builder=resume_builder,
                                                                  job_builder=job_builder))
            result[int(rid)][int(jid)] = match_degree

    return Response({
        "status": "success",
        "data": {
            'match_degree': result
        }
    }, status=status.HTTP_200_OK)


@api_view(['GET'])
@permission_classes([AllowAny])
def calculate_match_dimension_score(request) -> Response:
    """计算指定简历与岗位的指定维度评分"""
    # 解析简历ID
    resume_id_param = request.data.getlist('resume_id')
    if not resume_id_param:
        return Response(
            {
                'status': 'failed',
                'message': 'resume_id参数是必需的'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    resume_ids = parse_ids(resume_id_param)
    if resume_ids is None:
        return Response(
            {
                'status': 'failed',
                'message': 'resume_id必须是整数或整数列表'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    # 解析岗位ID
    job_id_param = request.data.getlist('job_id')
    if not job_id_param:
        return Response(
            {
                'status': 'failed',
                'message': 'job_id参数是必需的'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    job_ids = parse_ids(job_id_param)
    if job_ids is None:
        return Response(
            {
                'status': 'failed',
                'message': 'job_id必须是整数或整数列表'
            },
            status=status.HTTP_400_BAD_REQUEST
        )

    # 解析匹配维度
    match_dim_param = request.data.getlist('match_dimension')
    if not match_dim_param:
        match_dim_param = ["job_title_match", "skill_match", "work_experience_match",
                           "location_match", "salary_match", "education_match",
                           "project_experience_match", "other_factors"]

    # 处理单个维度或多个维度
    if isinstance(match_dim_param, list):
        match_dimensions = match_dim_param
    else:
        match_dimensions = [match_dim_param]

    # 验证维度是否有效
    for dim in match_dimensions:
        if dim not in ALLOWED_MATCH_DIMENSIONS:
            return Response(
                {
                    'status': 'failed',
                    'message': f"无效的匹配维度: '{dim}'，允许的维度为{ALLOWED_MATCH_DIMENSIONS}"
                },
                status=status.HTTP_400_BAD_REQUEST
            )

    # 后续计算操作
    resumes = Resume.objects.filter(resume_id__in=resume_ids)
    resumes = {resume['resume_id']: ResumeDataBuilder.from_dict(resume) for resume in
               ResumeSerializer(resumes, many=True).data}
    jobs = Job.objects.filter(job_id__in=job_ids)
    jobs = {job['job_id']: JobDataBuilder.from_dict(job) for job in JobSerializer(jobs, many=True).data}
    result: Dict[int, any] = {int(rid): {} for rid in resume_ids}
    for rid, resume_builder in resumes.items():
        for jid, job_builder in jobs.items():
            scores = (RJMatchDegreeCalculator(MatchDimensionWeightConfigBuilder.with_default_config())
                      .calculate_specified_dimension_score(resume_builder=resume_builder,
                                                           job_builder=job_builder,
                                                           dimensions=match_dimensions))
            result[int(rid)][int(jid)] = scores

    return Response({
        "status": "success",
        "data": {
            'dimension_score': result
        }
    }, status=status.HTTP_200_OK)
