from neomodel import (
    StringProperty,
    UniqueIdProperty,
    StructuredNode
)


class Attachment(StructuredNode):
    uid = UniqueIdProperty()
    title = StringProperty()
    hash = StringProperty()
    url = StringProperty()
    filetype = StringProperty()
