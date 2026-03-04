# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

# https://hl7.org/fhir/us/core/ValueSet-omb-race-category.html
omb_race_category = {
    constants.RACE.NATIVE_AMERICAN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "1002-5",
            "display": "American Indian or Alaska Native",
        },
    },
    constants.RACE.ASIAN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2028-9",
            "display": "Asian",
        },
    },
    constants.RACE.BLACK: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2054-5",
            "display": "Black or African American",
        },
    },
    constants.RACE.PACIFIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2076-8",
            "display": "Native Hawaiian or Other Pacific Islander",
        },
    },
    constants.RACE.WHITE: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2106-3",
            "display": "White",
        },
    },
    constants.COMMON.OTHER: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "OTH",
            "display": "other",
        },
    },
    constants.COMMON.NOT_AVAILABLE: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "NAVU",
            "display": "not available",
        },
    },
    constants.COMMON.NOT_REPORTED: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "NI",
            "display": "NoInformation",
        },
    },
    constants.COMMON.UNKNOWN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "UNK",
            "display": "unknown",
        },
    },
}

# https://hl7.org/fhir/us/core/ValueSet-omb-ethnicity-category.html
omb_ethnicity_category = {
    constants.ETHNICITY.HISPANIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2135-2",
            "display": "Hispanic or Latino",
        },
    },
    constants.ETHNICITY.NON_HISPANIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2186-5",
            "display": "Not Hispanic or Latino",
        },
    },
    constants.COMMON.NOT_AVAILABLE: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "NAVU",
            "display": "not available",
        },
    },
    constants.COMMON.NOT_REPORTED: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "NI",
            "display": "NoInformation",
        },
    },
    constants.COMMON.UNKNOWN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "UNK",
            "display": "unknown",
        },
    },
}

# http://hl7.org/fhir/R4/codesystem-administrative-gender.html
administrative_gender_code = {
    constants.GENDER.MALE: "male",
    constants.GENDER.FEMALE: "female",
    constants.COMMON.NOT_AVAILABLE: "unknown",
    constants.COMMON.NOT_REPORTED: "unknown",
    constants.COMMON.UNKNOWN: "unknown",
    constants.COMMON.OTHER: "other",
}
