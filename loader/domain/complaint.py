"""Define the Classes for Complaints."""
from loader.domain.types.enums import PropertyEnum
from loader.domain.source import Citation
from neomodel import (
    AsyncStructuredNode,
    AsyncStructuredRel,
    StringProperty,
    AsyncRelationshipTo,
    AsyncRelationship,
    AsyncRelationshipFrom,
    DateProperty,
    UniqueIdProperty,
    One
)


class RecordType(str, PropertyEnum):
    legal = "legal"
    news = "news"
    government = "government"
    personal = "personal"


# Neo4j Models
class ComplaintSourceRel(AsyncStructuredRel):
    uid = UniqueIdProperty()
    record_type = StringProperty(
        choices=RecordType.choices(),
        required=True
    )
    date_published = DateProperty()

    # Legal Source Properties
    court = StringProperty()
    judge = StringProperty()
    docket_number = StringProperty()
    case_event_date = DateProperty()

    # News Source Properties
    publication_name = StringProperty()
    publication_url = StringProperty()
    author = StringProperty()
    author_url = StringProperty()
    author_email = StringProperty()

    # Government Source Properties
    reporting_agency = StringProperty()
    reporting_agency_url = StringProperty()
    reporting_agency_email = StringProperty()


class Location(AsyncStructuredNode):
    location_type = StringProperty()
    loocation_description = StringProperty()
    address = StringProperty()
    city = StringProperty()
    state = StringProperty()
    zip = StringProperty()
    administrative_area = StringProperty()
    administrative_area_type = StringProperty()

    city_node = AsyncRelationshipTo(
        "loader.domain.infra.locations.CityNode", 
        "LOCATED_IN", cardinality=One)


class Complaint(AsyncStructuredNode):
    uid = UniqueIdProperty()
    record_id = StringProperty(index=True)
    complaint_key = StringProperty(unique_index=True)
    category = StringProperty()
    incident_date = DateProperty(index=True)
    received_date = DateProperty(index=True)
    closed_date = DateProperty(index=True)
    reason_for_contact = StringProperty()
    outcome_of_contact = StringProperty()

    # Relationships
    source_org = AsyncRelationshipTo("loader.domain.source.Source", "HAS_SOURCE", model=ComplaintSourceRel)
    location = AsyncRelationshipTo("Location", "OCCURRED_IN", cardinality=One)
    civilian_witnesses = AsyncRelationshipTo("loader.domain.civilian.Civilian", "WITNESSED")
    police_witnesses = AsyncRelationshipTo("loader.domain.officer.Officer", "WITNESSED")
    attachments = AsyncRelationshipTo("loader.domain.attachment.Attachment", "ATTACHED_TO")
    citations = AsyncRelationshipTo(
        'loader.domain.source.Source', "UPDATED_BY", model=Citation)
    # civilian_review_board = AsyncRelationshipFrom("CivilianReviewBoard", "REVIEWED")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Complaint {self.uid}>"


class Allegation(AsyncStructuredNode):
    uid = UniqueIdProperty()
    record_id = StringProperty(index=True)
    allegation_key = StringProperty(unique_index=True)
    allegation = StringProperty()
    type = StringProperty()
    subtype = StringProperty()
    recommended_finding = StringProperty()
    recommended_outcome = StringProperty()
    finding = StringProperty()
    outcome = StringProperty()

    # Relationships
    complainant = AsyncRelationshipTo("loader.domain.civilian.Civilian", "REPORTED_BY")
    accused = AsyncRelationshipFrom("loader.domain.officer.Officer", "ACCUSED_OF")
    complaint = AsyncRelationshipFrom("Complaint", "ALLEGED")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Allegation {self.uid}>"


class Investigation(AsyncStructuredNode):
    uid = UniqueIdProperty()
    start_date = DateProperty()
    end_date = DateProperty()

    # Relationships
    investigator = AsyncRelationship("loader.domain.officer.Officer", "LED_BY")
    complaint = AsyncRelationship("Complaint", "EXAMINED_BY")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Investigation {self.uid}>"


class Penalty(AsyncStructuredNode):
    uid = UniqueIdProperty()
    penalty = StringProperty()
    date_assessed = DateProperty()
    crb_plea = StringProperty()
    crb_case_status = StringProperty()
    crb_disposition = StringProperty()
    agency_disposition = StringProperty()

    # Relationships
    officer = AsyncRelationship("loader.domain.officer.Officer", "RECEIVED")
    complaint = AsyncRelationship("Complaint", "RESULTS_IN")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Penalty {self.uid}>"
