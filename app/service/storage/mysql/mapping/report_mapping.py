from app.service.storage.mysql.mapping.utils import split_list
from app.service.storage.mysql.schema.table import ReportTable
from app.domain.report import Report
from app.context import get_context

def map_to_report_table(report: Report) -> ReportTable:
    context = get_context()
    return ReportTable(
        id=report.id,
        name=report.name,
        description=report.description,
        tags=",".join(report.tags),
        index=report.index,
        query=report.query,
        enabled=report.enabled,

        tenant=context.tenant,
        production=context.production,
    )

def map_to_report(report_table: ReportTable) -> Report:
    return Report(
        id=report_table.id,
        name=report_table.name,
        description=report_table.description,
        tags=split_list(report_table.tags),
        index=report_table.index,
        query=report_table.query,
        enabled=report_table.enabled,
        production=report_table.production,
        running=report_table.running
    )
