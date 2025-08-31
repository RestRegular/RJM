from app.domain.test import Test
from app.service.storage.mysql.mapping.test_mapping import map_to_test_table
from app.service.storage.mysql.schema.table import TestTable
from app.service.storage.mysql.service.table_service import TableService
from app.service.storage.mysql.service.table_filtering import where_with_context
from app.service.storage.mysql.utils.select_result import SelectResult


# --------------------------------------------------------
# This Service Runs in Production and None-Production Mode
# It is PRODUCTION CONTEXT-LESS
# --------------------------------------------------------

def _where_with_context(*clause):
    return where_with_context(
        TestTable,
        False,
        *clause
    )


class TestService(TableService):

    async def load_all(self, search: str = None, limit: int = None, offset: int = None) -> SelectResult:
        if search:
            where = _where_with_context(
                TestTable.name.like(f'%{search}%')
            )
        else:
            where = _where_with_context()

        return await self._select_query(TestTable,
                                        where=where,
                                        order_by=TestTable.name,
                                        limit=limit,
                                        offset=offset)

    async def load_by_id(self, test_id: str) -> SelectResult:
        return await self._load_by_id(TestTable, primary_id=test_id, server_context=False)

    async def delete_by_id(self, test_id: str) -> tuple:
        return await self._delete_by_id(TestTable,
                                        primary_id=test_id,
                                        server_context=False)

    async def upsert(self, test: Test):
        return await self._replace(TestTable, map_to_test_table(test))
