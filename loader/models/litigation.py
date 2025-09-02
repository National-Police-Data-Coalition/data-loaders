from loader.models.types.enums import PropertyEnum
from neomodel import (
    StructuredNode,
    StringProperty,
    RelationshipTo,
    DateProperty,
    UniqueIdProperty
)


class LegalCaseType(str, PropertyEnum):
    CIVIL = "CIVIL"
    CRIMINAL = "CRIMINAL"


class CourtLevel(str, PropertyEnum):
    MUNICIPAL_OR_COUNTY = "Municipal or County"
    STATE_TRIAL = "State Trial Court"
    STATE_INTERMEDIATE_APPELLATE = "State Intermediate Appellate"
    STATE_HIGHEST = "State Highest"
    FEDERAL_DISTRICT = "Federal District"
    FEDERAL_APPELLATE = "Federal Appellate"
    US_SUPREME_COURT = "U.S. Supreme"


class Litigation(StructuredNode):
    uid = UniqueIdProperty()
    case_title = StringProperty()
    docket_number = StringProperty()
    court_name = StringProperty()
    court_level = StringProperty(choices=CourtLevel.choices())
    jurisdiction = StringProperty()
    state = StringProperty()
    description = StringProperty()
    start_date = DateProperty()
    settlement_date = DateProperty()
    settlement_amount = StringProperty()
    url = StringProperty()
    case_type = StringProperty(choices=LegalCaseType.choices())

    # Relationships
    documents = RelationshipTo("Document", "RELATED_TO")
    dispositions = RelationshipTo("Disposition", "YIELDED")
    defendants = RelationshipTo("Officer", "NAMED_IN")

    def __repr__(self):
        return f"<Litigation {self.uid}:{self.case_title}>"


class Document(StructuredNode):
    uid = UniqueIdProperty()
    title = StringProperty()
    description = StringProperty()
    url = StringProperty()


class Disposition(StructuredNode):
    description = StringProperty()
    date = DateProperty()
    disposition = StringProperty()
