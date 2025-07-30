from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import JobViewSet, batch_upload_job_data


router = DefaultRouter()
router.register(r'jobs', JobViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('job/bat_upload', batch_upload_job_data)
]
