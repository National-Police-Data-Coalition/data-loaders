"""Define the Classes for Complaints."""
from models.types.enums import PropertyEnum
# from models.source import Citation
from neomodel import (
    StructuredNode,
    StructuredRel,
    StringProperty,
    RelationshipTo,
    RelationshipFrom,
    DateProperty,
    UniqueIdProperty
)


class RecordType(str, PropertyEnum):
    legal = "legal"
    news = "news"
    government = "government"
    personal = "personal"


# Neo4j Models
class BaseSourceRel(StructuredRel):
    record_type = StringProperty(
        choices=RecordType.choices(),
        required=True
    )


class LegalSourceRel(BaseSourceRel):
    court = StringProperty()
    judge = StringProperty()
    docket_number = StringProperty()
    date_of_action = DateProperty()


class NewsSourceRel(BaseSourceRel):
    publication_name = StringProperty()
    publication_date = DateProperty()
    publication_url = StringProperty()
    author = StringProperty()
    author_url = StringProperty()
    author_email = StringProperty()


class GovernmentSourceRel(BaseSourceRel):
    reporting_agency = StringProperty()
    reporting_agency_url = StringProperty()
    reporting_agency_email = StringProperty()


class Location(StructuredNode):
    location_type = StringProperty()
    loocation_description = StringProperty()
    address = StringProperty()
    city = StringProperty()
    state = StringProperty()
    zip = StringProperty()
    responsibility = StringProperty()
    responsibility_type = StringProperty()


class Complaint(StructuredNode):
    uid = UniqueIdProperty()
    record_id = StringProperty()
    category = StringProperty()
    incident_date = DateProperty()
    recieved_date = DateProperty()
    closed_date = DateProperty()
    reason_for_contact = StringProperty()
    outcome_of_contact = StringProperty()

    # Relationships
    source_org = RelationshipFrom("models.source.Source", "REPORTED", model=BaseSourceRel)
    location = RelationshipTo("Location", "OCCURRED_AT")
    civlian_witnesses = RelationshipFrom("models.civilian.Civilian", "WITNESSED")
    police_witnesses = RelationshipFrom("models.officer.Officer", "WITNESSED")
    attachments = RelationshipTo("models.attachment.Attachment", "ATTACHED_TO")
    allegations = RelationshipTo("Allegation", "ALLEGED")
    investigations = RelationshipTo("Investigation", "EXAMINED_BY")
    penalties = RelationshipTo("Penalty", "RESULTS_IN")
    # citations = RelationshipTo(
    #     'models.source.Source', "UPDATED_BY", model=Citation)
    # civilian_review_board = RelationshipFrom("CivilianReviewBoard", "REVIEWED")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Complaint {self.uid}>"


class Allegation(StructuredNode):
    uid = UniqueIdProperty()
    record_id = StringProperty()
    allegation = StringProperty()
    type = StringProperty()
    subtype = StringProperty()
    recommended_finding = StringProperty()
    recommended_outcome = StringProperty()
    finding = StringProperty()
    outcome = StringProperty()

    # Relationships
    complainant = RelationshipTo("models.civilian.Civilian", "REPORTED_BY")
    accused = RelationshipFrom("models.officer.Officer", "ACCUSED_OF")
    complaint = RelationshipFrom("Complaint", "ALLEGED")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Allegation {self.uid}>"


class Investigation(StructuredNode):
    uid = UniqueIdProperty()
    start_date = DateProperty()
    end_date = DateProperty()

    # Relationships
    investigator = RelationshipFrom("models.officer.Officer", "LED_BY")
    complaint = RelationshipFrom("Complaint", "EXAMINED_BY")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Investigation {self.uid}>"


class Penalty(StructuredNode):
    uid = UniqueIdProperty()
    penalty = StringProperty()
    date_assessed = DateProperty()
    crb_plea = StringProperty()
    crb_case_status = StringProperty()
    crb_disposition = StringProperty()
    agency_disposition = StringProperty()

    # Relationships
    officer = RelationshipFrom("models.officer.Officer", "RECEIVED")
    complaint = RelationshipFrom("Complaint", "RESULTS_IN")

    def __repr__(self):
        """Represent instance as a unique string."""
        return f"<Penalty {self.uid}>"
