from neomodel import (
    StringProperty,
    UniqueIdProperty,
    AsyncStructuredNode
)


class Attachment(AsyncStructuredNode):
    uid = UniqueIdProperty()
    title = StringProperty()
    hash = StringProperty()
    url = StringProperty()
    filetype = StringProperty()
