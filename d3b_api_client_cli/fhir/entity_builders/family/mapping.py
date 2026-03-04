# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

type_code = {
    constants.SPECIES.DOG: "animal",
    constants.SPECIES.FLY: "animal",
    constants.SPECIES.HUMAN: "person",
    constants.SPECIES.MOUSE: "animal",
}
