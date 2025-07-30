from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import JobViewSet, upload_job_data


router = DefaultRouter()
router.register(r'jobs', JobViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('upload/', upload_job_data),
]
