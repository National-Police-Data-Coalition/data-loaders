from enum import Enum
"""Enumerations shared across multiple models are defined here."""


# Update Enums to work well with NeoModel
class PropertyEnum(Enum):
    """Use this Enum to convert the options to a dictionary."""
    @classmethod
    def choices(cls):
        return {item.value: item.name for item in cls}


class Gender(str, PropertyEnum):
    OTHER = 'Other'
    MALE = 'Male'
    FEMALE = 'Female'


class Ethnicity(str, PropertyEnum):
    WHITE = 'White'
    BLACK_AFRICAN_AMERICAN = 'Black/African American'
    AMERICAN_INDIAN_ALASKA_NATIVE = 'American Indian/Alaska Native'
    ASIAN = 'Asian'
    NATIVE_HAWAIIAN_PACIFIC_ISLANDER = 'Native Hawaiian/Pacific Islander'
    HISPANIC_LATINO = 'Hispanic/Latino'

    @classmethod
    def map_ethnicity(cls, ethnicity):
        ethnicity = ethnicity.strip().lower()
        for member in cls:
            if member.value.lower() == ethnicity:
                return member
        return None


class State(str, PropertyEnum):
    AL = "AL"
    AK = "AK"
    AZ = "AZ"
    AR = "AR"
    CA = "CA"
    CO = "CO"
    CT = "CT"
    DE = "DE"
    FL = "FL"
    GA = "GA"
    HI = "HI"
    ID = "ID"
    IL = "IL"
    IN = "IN"
    IA = "IA"
    KS = "KS"
    KY = "KY"
    LA = "LA"
    ME = "ME"
    MD = "MD"
    MA = "MA"
    MI = "MI"
    MN = "MN"
    MS = "MS"
    MO = "MO"
    MT = "MT"
    NE = "NE"
    NV = "NV"
    NH = "NH"
    NJ = "NJ"
    NM = "NM"
    NY = "NY"
    NC = "NC"
    ND = "ND"
    OH = "OH"
    OK = "OK"
    OR = "OR"
    PA = "PA"
    RI = "RI"
    SC = "SC"
    SD = "SD"
    TN = "TN"
    TX = "TX"
    UT = "UT"
    VT = "VT"
    VA = "VA"
    WA = "WA"
    WV = "WV"
    WI = "WI"
    WY = "WY"
    DC = "DC"
    PR = "PR"
    VI = "VI"
    GU = "GU"
