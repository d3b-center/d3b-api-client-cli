# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

# http://terminology.hl7.org/ValueSet/v3-FamilyMember
code_coding = {
    "Aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "AUNT",
        "display": "aunt",
    },
    constants.RELATIONSHIP.BROTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "BRO",
        "display": "brother",
    },
    "Brother-in-law": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "BROINLAW",
        "display": "brother-in-law",
    },
    "Brother-Monozygotic Twin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWINBRO",
        "display": "twin brother",
    },
    constants.RELATIONSHIP.CHILD: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "CHILD",
        "display": "child",
    },
    "Cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "COUSN",
        "display": "cousin",
    },
    constants.RELATIONSHIP.DAUGHTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "DAUC",
        "display": "daughter",
    },
    "father": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "FTH",
        "display": "father",
    },
    constants.RELATIONSHIP.FATHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "FTH",
        "display": "father",
    },
    "First cousin once removed": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    constants.RELATIONSHIP.GRANDCHILD: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDCHILD",
        "display": "grandchild",
    },
    constants.RELATIONSHIP.GRANDDAUGHTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDDAU",
        "display": "granddaughter",
    },
    constants.RELATIONSHIP.GRANDFATHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRFTH",
        "display": "grandfather",
    },
    constants.RELATIONSHIP.GRANDMOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRMTH",
        "display": "grandmother",
    },
    constants.RELATIONSHIP.GRANDSON: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDSON",
        "display": "grandson",
    },
    "Great Nephew": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    constants.RELATIONSHIP.HUSBAND: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "HUSB",
        "display": "husband",
    },
    "Married in aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Married in Husband": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "HUSB",
        "display": "husband",
    },
    "Married in-Spouse": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SPS",
        "display": "spouse",
    },
    "Maternal aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MAUNT",
        "display": "maternal aunt",
    },
    "Maternal Aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MAUNT",
        "display": "maternal aunt",
    },
    "Maternal cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MCOUSN",
        "display": "maternal cousin",
    },
    "Maternal Cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MCOUSN",
        "display": "maternal cousin",
    },
    "Maternal grandfather": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRFTH",
        "display": "maternal grandfather",
    },
    constants.RELATIONSHIP.MATERNAL_GRANDDAUGHTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDDAU",
        "display": "granddaughter",
    },
    constants.RELATIONSHIP.MATERNAL_GRANDFATHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRFTH",
        "display": "maternal grandfather",
    },
    "Maternal grandmother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRMTH",
        "display": "maternal grandmother",
    },
    constants.RELATIONSHIP.MATERNAL_GRANDMOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRMTH",
        "display": "maternal grandmother",
    },
    "Maternal great aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Great Aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Great Aunt (Mother's paternal aunt)": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Great Grandmother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGGRMTH",
        "display": "maternal great-grandmother",
    },
    "Maternal Great Uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal half-sister": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Relation": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MUNCLE",
        "display": "maternal uncle",
    },
    "mother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MTH",
        "display": "mother",
    },
    constants.RELATIONSHIP.MOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MTH",
        "display": "mother",
    },
    "Nephew": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "NEPHEW",
        "display": "nephew",
    },
    "Niece": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "NIECE",
        "display": "niece",
    },
    "Paternal aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PAUNT",
        "display": "paternal aunt",
    },
    "Paternal cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PCOUSN",
        "display": "paternal cousin",
    },
    "Paternal Cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PCOUSN",
        "display": "paternal cousin",
    },
    "Paternal grandfather": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PGRFTH",
        "display": "paternal grandfather",
    },
    "Paternal grandmother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PGRMTH",
        "display": "paternal grandmother",
    },
    constants.RELATIONSHIP.PATERNAL_GRANDMOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PGRMTH",
        "display": "paternal grandmother",
    },
    "Paternal uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PUNCLE",
        "display": "paternal uncle",
    },
    constants.RELATIONSHIP.PROBAND: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "CHILD",
        "display": "child",
    },
    constants.RELATIONSHIP.SIBLING: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SIB",
        "display": "sibling",
    },
    constants.RELATIONSHIP.SISTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SIS",
        "display": "sister",
    },
    constants.RELATIONSHIP.SON: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SONC",
        "display": "son",
    },
    constants.RELATIONSHIP.SPOUSE: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SPS",
        "display": "spouse",
    },
    "Twin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWIN",
        "display": "twin",
    },
    constants.RELATIONSHIP.TWIN_BROTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWINBRO",
        "display": "twin brother",
    },
    constants.RELATIONSHIP.TWIN_SISTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWINSIS",
        "display": "twin sister",
    },
    "Uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "UNCLE",
        "display": "uncle",
    },
    "Uncle-married in": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Wife": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "WIFE",
        "display": "wife",
    },
    constants.COMMON.OTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
        "code": "OTH",
        "display": "other",
    },
}
