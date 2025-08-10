from typing import Union, Tuple

from app.domain.flat_profile import FlatProfile
from app.domain.profile import *
from app.domain.storage_record import StorageRecord, StorageRecords
from app.exceptions.log_handler import get_logger
from app.service.storage.elastic.interface import raw as raw_db
from app.service.storage.elastic.driver.elastic_storage import ElasticFiledSort
from app.service.storage.elastic.driver.factory import storage_manager

logger = get_logger(__name__)

async def count_profile_duplicates(profile_ids: List[str]):
    return await storage_manager('profile').count({
        "query": {
            "bool": {
                "should": [
                    {
                        "terms": {
                            "ids": profile_ids
                        }
                    },
                    {
                        "terms": {
                            "id": profile_ids
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    })

async def load_profiles_with_duplicated_ids(log_error=True):
    query = {
        "size": 0,
        "aggs": {
            "duplicate_ids": {
                "terms": {
                    "field": "ids",
                    "min_doc_count": 2,
                    "size": 1000
                }
            }
        }
    }

    records = await storage_manager('profile').query(query, log_error)

    duplicated_ids = set()
    for data in records.aggregations("duplicate_ids").buckets():
        logger.info(f"Found {data['doc_count']} profiles with the same ID='{data['key']}'")
        duplicated_ids.add(data['key'])

    if duplicated_ids:
        async for row in load_by_ids(list(duplicated_ids), batch=1000):
            yield row


async def load_by_id(profile_id: str) -> Optional[StorageRecord]:
    query = {
        "size": 2,
        "query": {
            "bool": {
                "should": [
                    {
                        "term": {
                            "ids": profile_id
                        }
                    },
                    {
                        "term": {
                            "id": profile_id
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [
            {
                "metadata.time.update": {
                    "order": "desc"
                }
            }
        ]
    }

    profile_records = await storage_manager('profile').query(query)

    if profile_records.total <= 0:
        return None

    if profile_records.total > 1:
        logger.warning(
            "Profile {} id duplicated in the database. It will be merged with APM worker.".format(profile_id))

    return profile_records.first()


def load_by_ids(profile_ids: List[str], batch):
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "terms": {
                            "ids": profile_ids
                        }
                    },
                    {
                        "terms": {
                            "id": profile_ids
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }

    return storage_manager('profile').scan(query, batch)


async def load_modified_top_profiles(size):
    query = {
        "size": size,
        "sort": [
            {
                "metadata.time.update": {
                    "order": "desc"
                }
            }
        ]
    }
    return await storage_manager('profile').query(query)


async def load_by_primary_ids(profile_ids: List[str], size):
    query = {
        "size": size,
        "query": {
            "terms": {
                "id": profile_ids
            }
        }
    }
    return await storage_manager('profile').query(query)


async def load_all(start: int = 0, limit: int = 100, sort: List[Dict[str, Dict]] = None):
    return await storage_manager('profile').load_all(start, limit, sort)


async def save(profile: Union[FlatProfile, Profile, List[Profile], Set[Profile]], refresh_after_save=False):
    if isinstance(profile, (list, set)):
        for _profile in profile:
            if isinstance(_profile, Profile):
                _profile.mark_for_update()
    elif isinstance(profile, Profile):
        profile.mark_for_update()
    result = await storage_manager('profile').upsert(profile, exclude={"operation": ...})
    if refresh_after_save:
        await storage_manager('profile').flush()
    return result


async def save_all(profiles: List[Profile]):
    return await storage_manager("profile").upsert(profiles, exclude={"operation": ...})


async def refresh():
    return await storage_manager('profile').refresh()


async def flush():
    return await storage_manager('profile').flush()


async def delete_by_id(id: str, index: str):
    sm = storage_manager('profile')
    return await sm.delete(id, index)


async def bulk_delete_by_id(ids: List[str]):
    sm = storage_manager('profile')
    return await sm.bulk_delete(ids)


def scan(query: dict = None, batch: int = 1000):
    return storage_manager('profile').scan(query, batch)


def query(query: dict = None):
    return storage_manager('profile').query(query)


async def load_by_query_string(query: str):
    return await storage_manager('profile').load_by_query_string(query)


async def update_by_query(query, conflicts: str = 'proceed', wait_for_completion: bool = False):
    return await storage_manager('profile').update_by_query(
        query=query,
        conflicts=conflicts,
        wait_for_completion=wait_for_completion
    )


async def count(query: dict = None) -> dict:
    return await storage_manager('profile').count(query)


async def load_profile_by_values(key_value_pairs: List[Tuple[str, str]],
                                 sort_by: Optional[List[ElasticFiledSort]] = None,
                                 limit: int = 20) -> StorageRecords:
    return await raw_db.load_by_key_value_pairs('profile', key_value_pairs, sort_by, limit=limit)


async def load_profiles_by_segments(segments: List[str], condition: str = 'must'):
    """
    Requires all segments
    """
    return await storage_manager('profile').load_by_values(
        field_value_pairs=[('segments', segment) for segment in segments],
        condition=condition
    )


async def load_active_profile_by_field(field: str, value: str, start: int = 0, limit: int = 100) -> StorageRecords:
    query = {
        "from": start,
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            field: value
                        }
                    },
                    {
                        "term": {
                            "active": True
                        }
                    }
                ]
            }
        }
    }
    return await storage_manager('profile').query(query)


async def aggregate_by_field(bucket, aggr_field: str, query: dict = None, bucket_size: int = 100,
                             min_docs_count: int = 1):
    _query = {
        "size": 0,
        "aggs": {
            bucket: {
                "terms": {
                    "field": aggr_field,
                    "size": bucket_size,
                    "min_doc_count": min_docs_count
                }
            }
        }
    }

    if query:
        _query['query'] = query

    return await storage_manager('profile').query(_query)


# async def load_duplicated_profiles_for_profile(profile: Profile) -> StorageRecords:
#     if isinstance(profile.ids, list):
#         set(profile.ids).add(profile.id)
#         profile_ids = list(profile.ids)
#     else:
#         profile_ids = [profile.id]
#
#     return await load_profile_duplicates(profile_ids)


# async def load_duplicated_profiles_with_merge_key(merge_by: List[Tuple[str, str]]) -> StorageRecords:
#     return await storage_manager('profile').load_by_values(
#         merge_by,
#         condition='must',
#         limit=10000)
