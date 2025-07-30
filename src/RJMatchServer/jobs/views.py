from django.shortcuts import render
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny

from .models import Job
from .serializers import JobSerializer

from function_api import *


class JobViewSet(viewsets.ModelViewSet):
    queryset = Job.objects.all()
    serializer_class = JobSerializer


@api_view(['POST'])
@permission_classes([AllowAny])
def upload_job_data(request):
    if not request.data or 'job_ids' not in request.data:
        return Response({
            "error": "缺少参数",
            "message": "请提供参数 'job_ids'"
        }, status=status.HTTP_400_BAD_REQUEST)

    job_ids = request.data['job_ids']
    print('job_ids:', job_ids)
    if not isinstance(job_ids, list):
        return Response({
            "error": "参数错误",
            "message": "参数 'job_ids' 必须是列表"
        }, status=status.HTTP_400_BAD_REQUEST)
    for job_id in job_ids:
        if not isinstance(job_id, int):
            return Response(
                {'error': f'"job_ids"中的元素必须是整数，发现非整数元素: {job_id}'},
                status=status.HTTP_400_BAD_REQUEST
            )
    try:
        jobs = Job.objects.filter(job_id__in=job_ids)
        existing_ids = [job.job_id for job in jobs]
        non_existing_ids = [job_id for job_id in job_ids if job_id not in existing_ids]

        serializer = JobSerializer(list(jobs), many=True)
        job_list = serializer.data

        RJ_MATCHER.upload_job_datas(job_list)

        response_data = {
            "message": "成功上传存在的岗位数据",
            "processed_count": len(existing_ids),
            "non_existing_ids": non_existing_ids
        }
        return Response(response_data, status=status.HTTP_200_OK)
    except Exception as e:
        return Response(
            {'error': f'上传数据时发生错误: {str(e)}'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
