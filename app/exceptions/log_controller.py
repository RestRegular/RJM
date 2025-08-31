from typing import Optional

from contextlib import asynccontextmanager

from app.cluster_config import is_save_logs_on
from app.config import tracardi
from app.exceptions.log_handler import ElasticLogHandler


@asynccontextmanager
async def log_controller(log_handler: ElasticLogHandler) -> Optional[list]:
    if tracardi.save_logs and log_handler.has_logs():
        try:
            # Check global settings
            if await is_save_logs_on():
                yield log_handler.collection
            else:
                yield None

        finally:
            log_handler.reset()
    else:
        yield None