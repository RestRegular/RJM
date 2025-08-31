from app.context import get_context
from app.domain.version import Version
from app.service.storage.mysql.schema.table import VersionTable


def map_to_version_table(version: Version) -> VersionTable:
    context = get_context()

    return VersionTable(
        tenant=context.tenant,
        es_schema_version=version.db_version,
        api_version=version.version,
        mysql_schema_version=version.mysql_version
    )


def map_to_version(version_table: VersionTable) -> Version:
    return Version(
        version=version_table.api_version,
        db_version=version_table.es_schema_version,
        mysql_version=version_table.mysql_schema_version
    )
