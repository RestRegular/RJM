import json
import time
from typing import List, Any

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

_match_job_id = 0


def new_match_job_id() -> str:
    global _match_job_id
    _match_job_id += 1
    return f"resume-match-job-{_match_job_id}"


@api_view(['POST'])
@permission_classes([AllowAny])
def upload_resume_get_matched_jobs(request) -> Response:
    """
    上传简历数据并获取指定数量的岗位数据

    :param request: 必须包含 data，且 data 中包含以下字段:
                    必要: 'ids'(List[int]), 'num'(int), 'threshold'(int)
                    可选: 'time_out'(int)
    :return: Response
    """
    # 验证请求数据是否完整
    required_fields = ['ids', 'num', 'threshold']
    if not request.data or not all(field in request.data for field in required_fields):
        return Response({
            'status': 'failed',
            'message': f"请在请求参数中包含必要的字段: {required_fields}"
        }, status=status.HTTP_400_BAD_REQUEST)

    # 提取并验证参数类型
    resume_ids: List[int] = request.data['ids']
    num: int = request.data['num']
    threshold: int = request.data['threshold']
    time_out = request.data.get('time_out', 15)

    if not isinstance(resume_ids, list):
        return Response({
            'status': 'failed',
            'message': "'ids' 字段必须为 list 类型"
        }, status=status.HTTP_400_BAD_REQUEST)

    if not isinstance(num, int) or num < 0:
        return Response({
            'status': 'failed',
            'message': "'num' 字段必须为 int 类型且大于 0"
        }, status=status.HTTP_400_BAD_REQUEST)

    if not isinstance(threshold, int) or threshold < 0:
        return Response({
            'status': 'failed',
            'message': "'threshold' 字段必须为 int 类型且大于 0"
        }, status=status.HTTP_400_BAD_REQUEST)

    if not isinstance(time_out, int) or time_out < 0:
        return Response({
            'status': 'failed',
            'message': "'time_out' 字段必须为 int 类型且大于 0"
        }, status=status.HTTP_400_BAD_REQUEST)

    # 验证ids列表中的元素是否都是整数
    non_int_ids = [rid for rid in resume_ids if not isinstance(rid, int)]
    if non_int_ids:
        return Response({
            'status': 'failed',
            'message': f"'ids' 中的元素必须是整数，发现非整数元素: {non_int_ids}"
        }, status=status.HTTP_400_BAD_REQUEST)

    try:
        # 检查简历是否存在
        resumes = Resume.objects.filter(resume_id__in=resume_ids)
        existing_ids = {resume.resume_id for resume in resumes}
        non_existing_ids = [rid for rid in resume_ids if rid not in existing_ids]

        if non_existing_ids:
            return Response({
                'status': 'failed',
                'message': f"待匹配的简历数据不存在: {non_existing_ids}"
            }, status=status.HTTP_400_BAD_REQUEST)

        # 序列化简历数据
        serializer = ResumeSerializer(resumes, many=True)
        resume_data = serializer.data

        upload_resume_topic = new_upload_resume_topic()

        # 上传简历数据
        if not RJ_MATCHER.upload_resume_datas(upload_resume_topic, resume_data, True):
            return Response({
                'status': 'failed',
                'message': "上传简历数据时出现错误"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        set_match_threshold(threshold)

        # 配置并启动匹配任务
        match_job_id = new_match_job_id()
        RJ_MATCHER.add_match_job(
            job_id=match_job_id,
            source_topic=upload_resume_topic,
            flink_config={'parallelism': 1},  # 可修改的并行度配置
            match_functions=[ResumeMatcher(redis_config=RJ_MATCHER.get_redis_config())],
            stringify_function=lambda result: json.dumps(list(result)),
            in_debug_mode=False
        )

        match_thread = RJ_MATCHER.start(match_job_id)
        match_results: List[Any] = []
        hasFinished = False

        # 处理匹配结果的回调函数
        def handle_single_result(result) -> None:
            if len(match_results) >= num:
                if not hasFinished:
                    hasFinished = True
                    print(f"匹配结果订阅自动结束: [{match_job_id}]")
            else:
                match_results.append(result['value'])

        def handle_all_results() -> None:
            nonlocal hasFinished
            hasFinished = True
            print(f"匹配结果订阅自动结束: [{match_job_id}]")

        # 订阅匹配结果
        RJ_MATCHER.subscribe_result(
            job_id=match_job_id,
            time_out=1000,
            max_message_size=num,
            single_message_callback=handle_single_result,
            end_callback=handle_all_results,
            in_debug_mode=False
        )

        # 等待匹配完成或超时
        max_timeout_seconds = time_out
        while not hasFinished and max_timeout_seconds > 0:
            time.sleep(1)
            max_timeout_seconds -= 1

        if not hasFinished:
            print(f"匹配结果订阅超时结束: [{match_job_id}]")

        return Response({
            'status': 'success',
            'data': {'match_result': match_results[:num]}
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return Response({
            'status': 'failed',
            'message': f"匹配岗位数据时出现错误: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    finally:
        # 确保资源被正确清理，无论是否发生异常
        if 'match_job_id' in locals():
            if match_job_id in RJ_MATCHER.all_match_job_ids():
                RJ_MATCHER.stop_match(match_job_id, release_source_topic=True)
                RJ_MATCHER.cancel_subscribe_result(match_job_id)


@api_view(['POST'])
@permission_classes([AllowAny])
def upload_job_get_matched_resumes(request) -> Response:
    """

    :param request:
    :return:
    """
    # 验证请求数据是否完整
    required_fields = ['ids', 'num', 'threshold']
    if not request.data or not all(field in request.data for field in required_fields):
        return Response({
            'status': 'failed',
            'message': f"请在请求参数中包含必要的字段: {required_fields}"
        }, status=status.HTTP_400_BAD_REQUEST)

    # 提取并验证参数类型
    job_ids: List[int] = request.data['ids']
    num: int = request.data['num']
    threshold: int = request.data['threshold']
    time_out = request.data.get('time_out', 15)

    if not isinstance(job_ids, list):
        return Response({
            'status': 'failed',
            'message': "'ids' 字段必须为 list 类型"
        }, status=status.HTTP_400_BAD_REQUEST)

    if not isinstance(num, int) or num < 0:
        return Response({
            'status': 'failed',
            'message': "'num' 字段必须为 int 类型且大于 0"
        }, status=status.HTTP_400_BAD_REQUEST)

    if not isinstance(threshold, int) or threshold < 0:
        return Response({
            'status': 'failed',
            'message': "'threshold' 字段必须为 int 类型且大于 0"
        }, status=status.HTTP_400_BAD_REQUEST)

    if not isinstance(time_out, int) or time_out < 0:
        return Response({
            'status': 'failed',
            'message': "'time_out' 字段必须为 int 类型且大于 0"
        }, status=status.HTTP_400_BAD_REQUEST)

    # 验证ids列表中的元素是否都是整数
    non_int_ids = [rid for rid in job_ids if not isinstance(rid, int)]
    if non_int_ids:
        return Response({
            'status': 'failed',
            'message': f"'ids' 中的元素必须是整数，发现非整数元素: {non_int_ids}"
        }, status=status.HTTP_400_BAD_REQUEST)

    try:
        resumes = Resume.objects.all()
        seriallizer = ResumeSerializer(resumes, many=True)
        resumes = seriallizer.data
        upload_resume_topic = new_upload_resume_topic()
        if not RJ_MATCHER.upload_resume_datas(upload_resume_topic, resumes, True):
            return Response({
                'status': 'failed',
                'message': "上传简历数据时出现错误"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        set_match_threshold(threshold)

        # 配置并启动匹配任务
        match_job_id = new_match_job_id()
        RJ_MATCHER.add_match_job(
            job_id=match_job_id,
            source_topic=upload_resume_topic,
            flink_config={'parallelism': 1},  # 可修改的并行度配置
            match_functions=[JobMatcher(redis_config=RJ_MATCHER.get_redis_config(), job_ids=job_ids)],
            stringify_function=lambda result: json.dumps(list(result)),
            in_debug_mode=False
        )

        match_thread = RJ_MATCHER.start(match_job_id)
        match_results: List[Any] = []
        hasFinished = False

        # 处理匹配结果的回调函数
        def handle_single_result(result) -> None:
            if len(match_results) >= num:
                if not hasFinished:
                    hasFinished = True
                    print(f"匹配结果订阅自动结束: [{match_job_id}]")
            else:
                match_results.append(result['value'])

        def handle_all_results() -> None:
            nonlocal hasFinished
            hasFinished = True
            print(f"匹配结果订阅自动结束: [{match_job_id}]")

        # 订阅匹配结果
        RJ_MATCHER.subscribe_result(
            job_id=match_job_id,
            time_out=1000,
            max_message_size=num,
            single_message_callback=handle_single_result,
            end_callback=handle_all_results,
            in_debug_mode=False
        )

        # 等待匹配完成或超时
        max_timeout_seconds = time_out
        while not hasFinished and max_timeout_seconds > 0:
            time.sleep(1)
            max_timeout_seconds -= 1

        if not hasFinished:
            print(f"匹配结果订阅超时结束: [{match_job_id}]")

        return Response({
            'status': 'success',
            'data': {'match_result': match_results[:num]}
        }, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({
            'status': 'failed',
            'message': f"匹配简历数据时出现错误: {str(e)}"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    finally:
        # 确保资源被正确清理，无论是否发生异常
        if 'match_job_id' in locals():
            if match_job_id in RJ_MATCHER.all_match_job_ids():
                RJ_MATCHER.stop_match(match_job_id, release_source_topic=True)
                RJ_MATCHER.cancel_subscribe_result(match_job_id)
