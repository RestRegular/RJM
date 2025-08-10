from typing import List, Optional

from app.domain.named_entity import NamedEntityInContext


class FlowMetaData(NamedEntityInContext):
    description: str
    tags: Optional[List[str]] = ["General"]
    type: str = 'collection'
