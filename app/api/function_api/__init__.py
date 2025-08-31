from .api.resume_job_matcher import RJMatcher
from .components.flink_matching_processor import set_match_threshold

RJ_MATCHER = RJMatcher(
    redis_config={
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'decode_responses': True
    },
    kafka_config={
        'bootstrap_servers': 'localhost:9092',
        'client_id': 'job-matching-client'
    },
    empty_redis=True
)

RJ_MATCHER.set_kafka_producer_config(batch_size=16384, buffer_memory=33554432)

set_match_threshold(20)

_UPLOAD_RESUME_TOPIC = "resume-source-topic"
_upload_resume_topic_id = 0


def upload_all_job_datas():
    from jobs.models import Job
    from jobs.serializers import JobSerializer
    jobs = Job.objects.all()
    serializer = JobSerializer(jobs, many=True)
    jobs = serializer.data
    RJ_MATCHER.upload_job_datas(jobs)


def upload_all_resume_datas():
    from resumes.models import Resume
    from resumes.serializers import ResumeSerializer
    resumes = Resume.objects.all()
    serializer = ResumeSerializer(resumes, many=True)
    resumes = serializer.data
    RJ_MATCHER.upload_resume_datas_to_redis(resumes)


def new_upload_resume_topic():
    global _upload_resume_topic_id
    _upload_resume_topic_id += 1
    return f"{_UPLOAD_RESUME_TOPIC}-{_upload_resume_topic_id}"


upload_all_job_datas()
upload_all_resume_datas()
