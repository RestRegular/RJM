from django.urls import path, include
from .views import upload_resume_get_matched_jobs


urlpatterns = [
    path('matching/resume_to_job', upload_resume_get_matched_jobs)
]