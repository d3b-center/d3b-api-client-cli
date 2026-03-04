# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

from d3b_api_client_cli.fhir.entity_builders.specimen_common.mapping import *

status_mapping = {
    constants.SPECIMEN.STATUS.DISPOSED: "unavailable",
    constants.SPECIMEN.STATUS.NOT_AVAILABLE: "unavailable",
    constants.SPECIMEN.STATUS.ON_SITE: "available",
    constants.SPECIMEN.STATUS.OTHER: "unavailable",
    constants.SPECIMEN.STATUS.PATHOLOGY_GOVERNED: "unavailable",
    constants.SPECIMEN.STATUS.SHIPPED: "unavailable",
    constants.SPECIMEN.STATUS.SHIPPED_GENOMIC_DATA: "unavailable",
    constants.SPECIMEN.STATUS.UNKNOWN: "unavailable",
    constants.SPECIMEN.STATUS.VIRTUAL: "unavailable",
}
