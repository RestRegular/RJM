from typing import Optional

from app.domain.version import Version
from app.exceptions.log_handler import get_logger
from app.service.storage.mysql.mapping.version_mapping import map_to_version_table, map_to_version
from app.service.storage.mysql.schema.table import VersionTable
from app.service.storage.mysql.service.table_service import TableService
from app.service.storage.mysql.service.user_service import _where_with_context
from app.service.storage.mysql.utils.select_result import SelectResult

logger = get_logger(__name__)

# --------------------------------------------------------
# This Service Runs in Production and None-Production Mode
# It is PRODUCTION CONTEXT-LESS
# --------------------------------------------------------

class VersionService(TableService):

    async def load_all(self, limit: int = None, offset: int = None) -> SelectResult:
        where = _where_with_context()

        return await self._select_query(VersionTable,
                                        where=where,
                                        limit=limit,
                                        offset=offset)

    async def upsert(self, version: Version):
        return await self._replace(VersionTable, map_to_version_table(version))

    async def load_by_version(self, version: str) -> Optional[Version]:
        where = _where_with_context(  # tenant only mode
            VersionTable.api_version == version
        )

        records = await self._select_query(VersionTable, where=where)

        if not records.exists():
            return None

        return records.map_first_to_object(map_to_version)
