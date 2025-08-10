import logging

from app.domain.setting import Setting

from app.context import Context, ServerContext
from app.domain.bridge import Bridge
from app.domain.consent_field_compliance import EventDataCompliance
from app.domain.consent_type import ConsentType
from app.domain.destination import Destination
from app.domain.event_redirect import EventRedirect
from app.domain.event_reshaping_schema import EventReshapingSchema
from app.domain.event_source import EventSource
from app.domain.event_to_profile import EventToProfile
from app.domain.event_type_metadata import EventTypeMetadata
from app.domain.event_validator import EventValidator
from app.domain.flow import FlowRecord
from app.domain.identification_point import IdentificationPoint
from app.domain.import_config import ImportConfig
from app.domain.report import Report
from app.domain.resource import Resource, ResourceRecord
from app.domain.rule import Rule
from app.domain.storage_record import StorageRecord
from app.domain.task import Task
from app.domain.user import User
from app.exceptions.log_handler import get_installation_logger
from app.service.storage.mysql.mapping.bridge_mapping import map_to_bridge_table
from app.service.storage.mysql.mapping.consent_type_mapping import map_to_consent_type_table
from app.service.storage.mysql.mapping.destination_mapping import map_to_destination_table
from app.service.storage.mysql.mapping.event_data_compliance_mapping import map_to_event_data_compliance_table
from app.service.storage.mysql.mapping.event_redirect_mapping import map_to_event_redirect_table
from app.service.storage.mysql.mapping.event_reshaping_mapping import map_to_event_reshaping_table
from app.service.storage.mysql.mapping.event_source_mapping import map_to_event_source_table
from app.service.storage.mysql.mapping.event_to_event_mapping import map_to_event_mapping_table
from app.service.storage.mysql.mapping.event_to_profile_mapping import map_to_event_to_profile_table
from app.service.storage.mysql.mapping.event_validation_mapping import map_to_event_validation_table
from app.service.storage.mysql.mapping.identification_point_mapping import map_to_identification_point
from app.service.storage.mysql.mapping.import_mapping import map_to_import_config_table
from app.service.storage.mysql.mapping.report_mapping import map_to_report_table
from app.service.storage.mysql.mapping.resource_mapping import map_to_resource_table
from app.service.storage.mysql.mapping.setting_mapping import map_to_settings_table
from app.service.storage.mysql.mapping.task_mapping import map_to_task_table
from app.service.storage.mysql.mapping.user_mapping import map_to_user_table
from app.service.storage.mysql.mapping.workflow_mapping import map_to_workflow_table
from app.service.storage.mysql.mapping.workflow_trigger_mapping import map_to_workflow_trigger_table
from app.service.storage.mysql.schema.table import BridgeTable, ConsentTypeTable, \
    IdentificationPointTable, EventMappingTable, EventSourceTable, EventDataComplianceTable, EventReshapingTable, \
    TaskTable, UserTable, DestinationTable, EventRedirectTable, ReportTable, \
    EventToProfileMappingTable, WorkflowTriggerTable, ResourceTable, WorkflowTable, ImportTable, EventValidationTable, \
    SettingTable
from app.service.storage.mysql.service.table_service import TableService
from app.worker.domain.migration_schema import MigrationSchema
from app.worker.misc.base_64 import b64_decoder
from app.worker.misc.task_progress import task_create, task_progress, task_finish, task_status
from app.worker.service.worker.migration_workers.utils.client import ElasticClient

logger = get_installation_logger(__name__, level=logging.INFO)

def resource_converter(resource_record: ResourceRecord, schema: MigrationSchema) -> Resource:
    return resource_record.decode()


def destination_record_converter(record: StorageRecord, schema: MigrationSchema) -> StorageRecord:
    record['destination'] = b64_decoder(record['destination'])
    record['mapping'] = b64_decoder(record['mapping'])

    return record


def report_record_converter(record: StorageRecord, schema: MigrationSchema) -> StorageRecord:
    record['query'] = b64_decoder(record['query'])
    return record

def flow_record_converter(record: StorageRecord, schema: MigrationSchema) -> StorageRecord:
    record['production'] = False
    record['draft'] = b64_decoder(record['draft'])
    record['draft']['wf_schema']['version'] = schema.params['version']
    record['draft']['wf_schema']['server_version'] = schema.params['version']
    return record

def user_record_converter(record: StorageRecord, schema: MigrationSchema) -> StorageRecord:
    record['name'] = record.get('full_name',"Unknown")
    record['enabled'] = not record.get('disabled', True)
    return record



class_mapping = {
    "bridge": (Bridge, BridgeTable, map_to_bridge_table, None, None),
    "consent-type": (ConsentType, ConsentTypeTable, map_to_consent_type_table, None, None),
    "identification-point": (IdentificationPoint, IdentificationPointTable, map_to_identification_point, None, None),
    # "events-tags": (EventTypeMetadata, EventMappingTable, map_to_event_mapping_table, None, None),  # todo check
    "event-source": (EventSource, EventSourceTable, map_to_event_source_table, None, None),
    "consent-data-compliance": (
    EventDataCompliance, EventDataComplianceTable, map_to_event_data_compliance_table, None, None),
    "event-reshaping": (EventReshapingSchema, EventReshapingTable, map_to_event_reshaping_table, None, None),
    # "tracardi-pro": (None, TracardiProTable, None, None, None), # todo check
    "task": (Task, TaskTable, map_to_task_table, None, None),
    "event-management": (EventTypeMetadata, EventMappingTable, map_to_event_mapping_table, None, None),
    "user": (User, UserTable, map_to_user_table, user_record_converter, None),
    "destination": (Destination, DestinationTable, map_to_destination_table, destination_record_converter, None),
    "event-redirect": (EventRedirect, EventRedirectTable, map_to_event_redirect_table, None, None),
    "report": (Report, ReportTable, map_to_report_table, report_record_converter, None),
    "event_to_profile": (EventToProfile, EventToProfileMappingTable, map_to_event_to_profile_table, None, None),
    "rule": (Rule, WorkflowTriggerTable, map_to_workflow_trigger_table, None, None),
    "resource": (ResourceRecord, ResourceTable, map_to_resource_table, None, resource_converter),
    "flow": (FlowRecord, WorkflowTable, map_to_workflow_table, flow_record_converter, None),
    "import": (ImportConfig, ImportTable, map_to_import_config_table, None, None),
    "event-validation": (EventValidator, EventValidationTable, map_to_event_validation_table, None, None),
    "setting": (Setting, SettingTable, map_to_settings_table, None, None),
}


async def copy_to_mysql(schema: MigrationSchema, elastic_host: str, context: Context):
    if schema.copy_index.multi is True:
        raise ValueError('Multi index can not be copied to mysql.')

    with ServerContext(context):
        task_id = await task_create(
            "upgrade",
            f"Migration of \"{schema.copy_index.from_index}\" to mysql table \"{schema.copy_index.to_index}\"",
            schema.model_dump()
        )

        storage_class = schema.params['mysql']
        if storage_class not in class_mapping:
            logger.warning(f'Could not find class {storage_class}. Data not migrated.')
            return

        domain_type, object_table, domain_object_mapping_to_table, record_converter, domain_converter = class_mapping[
            storage_class]

        chunk = 100
        moved_records = 0

        with ElasticClient(hosts=[elastic_host]) as client:
            while records_to_move := client.load_records(
                    index=schema.copy_index.from_index,
                    start=moved_records,
                    size=chunk
            ):

                for number, record in enumerate(records_to_move):  # Type: int,StorageRecord

                    try:

                        if record_converter is not None:
                            record = record_converter(record, schema)

                        domain_object = record.to_entity(domain_type, set_metadata=False)

                        if domain_converter is not None:
                            domain_object = domain_converter(domain_object, schema)

                        table_data = domain_object_mapping_to_table(domain_object)

                        ts = TableService()
                        result = await ts._replace(object_table, table_data)

                    except Exception as e:
                        await task_status(task_id, 'error', str(e))
                        logger.error(str(e))
                        continue

                    # Progress

                    await task_progress(task_id, number)  # It is progress for each chunk

                moved_records += chunk

        await task_finish(task_id)

        logger.info(f"Data migrated from {schema.copy_index.from_index}")
