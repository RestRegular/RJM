CACHE_ID = 0


def get_cache_id() -> int:
    global CACHE_ID
    CACHE_ID += 1
    return CACHE_ID