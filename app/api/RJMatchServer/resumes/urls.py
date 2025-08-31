from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ResumeViewSet, batch_upload_resume_data, batch_get_resume_data, multi_dimension_index_resumes


router = DefaultRouter()
router.register(r'resumes', ResumeViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('resume/bat_upload', batch_upload_resume_data),
    path('resume/bat_get', batch_get_resume_data),
    path('resume/search_resume', multi_dimension_index_resumes)
]
