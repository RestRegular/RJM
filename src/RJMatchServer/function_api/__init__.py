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
    }
)

matcher.set_kafka_producer_config(batch_size=16384, buffer_memory=33554432)

set_match_threshold(20)
