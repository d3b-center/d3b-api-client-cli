# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

# http://terminology.hl7.org/CodeSystem/v2-0136
value_coding = {
    constants.COMMON.TRUE: {
        "system": "http://terminology.hl7.org/CodeSystem/v2-0136",
        "code": "Y",
        "display": "Yes",
    },
    constants.COMMON.FALSE: {
        "system": "http://terminology.hl7.org/CodeSystem/v2-0136",
        "code": "N",
        "display": "No",
    },
    None: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
        "code": "NI",
        "display": "NoInformation",
    },
}
