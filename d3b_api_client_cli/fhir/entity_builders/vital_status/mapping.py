# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

code_coding = {
    constants.OUTCOME.VITAL_STATUS.ALIVE: {
        "system": "http://snomed.info/sct",
        "code": "438949009",
        "display": "Alive (finding)",
    },
    constants.OUTCOME.VITAL_STATUS.DEAD: {
        "system": "http://snomed.info/sct",
        "code": "419099009",
        "display": "Dead (finding)",
    },
}
