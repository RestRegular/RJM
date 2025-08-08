from app.service.license import License

if License.has_license():
    import com_tracardi.storage.mysql.interface.destination as destination_dao
    import com_tracardi.storage.mysql.interface.resource as resource_dao
    import com_tracardi.storage.mysql.interface.event_source as event_source_dao
    import com_tracardi.storage.mysql.interface.event_validation as event_validation_dao
    import com_tracardi.storage.mysql.interface.event_mapping as event_mapping_dao
    import com_tracardi.storage.mysql.interface.event_to_profile_mapping as event_to_profile_dao
    import com_tracardi.storage.mysql.interface.event_reshaping as event_reshaping_dao
else:
    import app.service.storage.mysql.interface.destination as destination_dao
    import app.service.storage.mysql.interface.resource as resource_dao
    import app.service.storage.mysql.interface.event_source as event_source_dao
    import app.service.storage.mysql.interface.event_validation as event_validation_dao
    import app.service.storage.mysql.interface.event_mapping as event_mapping_dao
    import app.service.storage.mysql.interface.event_to_profile_mapping as event_to_profile_dao
    import app.service.storage.mysql.interface.event_reshaping as event_reshaping_dao

__all__ = [
    'destination_dao',
    'resource_dao',
    'event_source_dao',
    'event_validation_dao',
    'event_mapping_dao',
    'event_to_profile_dao',
    'event_reshaping_dao'
]
