# noqa

"""
Map a string from d3b_api_client_cli.fhir.constants to 
a FHIR coding type
"""

from d3b_api_client_cli.fhir import constants

category_coding = {
    "BIRTHDEFECT": {
        "system": "http://snomed.info/sct",
        "code": "276720006",
        "display": "Dysmorphism (disorder)",
    },
    "CANCER": {
        "system": "http://snomed.info/sct",
        "code": "86049000",
        "display": "Malignant neoplasm, primary (morphologic abnormality)",
    },
    "COVID19": {
        "system": "http://snomed.info/sct",
        "code": "840539006",
        "display": "Disease caused by Severe acute respiratory syndrome coronavirus 2",
    },
}
