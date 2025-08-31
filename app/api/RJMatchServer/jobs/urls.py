from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import JobViewSet, batch_upload_job_data, batch_get_job_data, multi_dimension_index_jobs


router = DefaultRouter()
router.register(r'jobs', JobViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('job/bat_upload', batch_upload_job_data),
    path('job/bat_get', batch_get_job_data),
    path('job/search_job', multi_dimension_index_jobs)
]
