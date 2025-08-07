from django.urls import path
from .views import upload_resume_get_matched_jobs, upload_job_get_matched_resumes


urlpatterns = [
    path('matching/resume_to_job', upload_resume_get_matched_jobs),
    path('matching/job_to_resume', upload_job_get_matched_resumes)
]