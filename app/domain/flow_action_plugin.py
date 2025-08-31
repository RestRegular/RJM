from datetime import datetime
from typing import Optional, Any, Callable
from app.service.plugin.domain.register import Plugin
from app.domain.entity import Entity
from app.domain.metadata import Metadata
from app.domain.settings import Settings
from app.domain.time import Time
from app.service.module_loader import import_package, load_callable
from app.service.utils.date import now_in_utc


class FlowActionPlugin(Entity):

    """
    This object can not be loaded without encoding.
    Load it as FlowActionPluginRecord and then decode.
    """

    metadata: Optional[Metadata] = None
    plugin: Plugin
    settings: Optional[Settings] = Settings()

    def __init__(self, **data: Any):
        data['metadata'] = Metadata(
            time=Time(
                insert=now_in_utc()
            ))
        super().__init__(**data)

    # Persistence


    def get_validator(self) -> Callable:
        module = import_package(self.plugin.spec.module)
        return load_callable(module, 'validate')
