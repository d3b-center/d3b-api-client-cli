# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

# http://hl7.org/fhir/ValueSet/condition-ver-status
verification_status_coding = {
    constants.PHENOTYPE.OBSERVED.YES: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "confirmed",
        "display": "Confirmed",
    },
    constants.PHENOTYPE.OBSERVED.NO: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "refuted",
        "display": "Refuted",
    },
    "default": {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "unconfirmed",
        "display": "unconfirmed",
    },
}
