"""
All configuration values for the CLI
"""

import os
from dataclasses import dataclass
from enum import Enum

from dotenv import find_dotenv, load_dotenv

# File paths and directories
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname((__file__))))
ROOT_DATA_DIR = os.path.join(ROOT_DIR, "data")
ROOT_FAKE_DATA_DIR = os.path.join(ROOT_DATA_DIR, "fake_data")
DATA_DIR = os.path.join(ROOT_DATA_DIR, "generated")
EXPORT_DIR = os.path.join(ROOT_DATA_DIR, "exported")
TRANSFORM_DIR = os.path.join(ROOT_DATA_DIR, "transformed")
DELETE_DIR = os.path.join(ROOT_DATA_DIR, "deleted")
LOAD_DIR = os.path.join(ROOT_DATA_DIR, "loaded")
STATS_DIR = os.path.join(ROOT_DATA_DIR, "stats")
DEWRANGLE_DIR = os.path.join(ROOT_DATA_DIR, "dewrangle")
FHIR_JSON_DIR = os.path.join(ROOT_DATA_DIR, "fhir")
LOG_DIR = os.path.join(ROOT_DATA_DIR, "logs")
SAMPLE_ETL_DIR = os.path.join(ROOT_DATA_DIR, "sample_etl")
SAMPLE_ETL_MANIFEST = "sample_etl_manifest.json"

# DB
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_USER_PW = os.environ.get("DB_USER_PW")

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

# Unit test environment variable
EXPORT_DIR_ENV_VAR = "DWDS_PYEXPORT_DIR_ENV_VAR"

# Dataservice
DEV_STUDY_ID = "SD_ME0WME0W"
TEST_STUDY_ID = "SD_11111111"
DATASERVICE_DB_NAME = os.environ.get("DATASERVICE_DB_NAME")
DATASERVICE_DB_HOST = os.environ.get("DATASERVICE_DB_HOST")
DATASERVICE_DB_PORT = os.environ.get("DATASERVICE_DB_PORT")
DATASERVICE_DB_ADMIN_USER = os.environ.get("POSTGRES_ADMIN_USER")
DATASERVICE_DB_ADMIN_PW = os.environ.get("POSTGRES_ADMIN_PW")

# Dewrangle
DEWRANGLE_DEV_PAT = os.environ.get("DEWRANGLE_DEV_PAT")
DEWRANGLE_BASE_URL = os.environ.get("DEWRANGLE_BASE_URL") or (
    "http://localhost:3000"
)
DEWRANGLE_FHIR_SERVERS_FILEPATH = os.path.join(ROOT_DIR, ".fhir_servers.json")
DEWRANGLE_MAX_PAGE_SIZE = 10

# KF Gen3 Service where file metadata is stored: Indexd
INDEXD_BASE_URL = os.environ.get("INDEXD_BASE_URL") or (
    "https://data.kidsfirstdrc.org"
)
# NCI Gen3 Service where file metadata is stored: DCF
DCF_BASE_URL = os.environ.get("DCF_BASE_URL") or (
    "https://nci-crdc.datacommons.io"
)
INDEXD_ENDPOINT = os.environ.get("INDEXD_ENDPOINT") or "index/index"

# FHIR
FHIR_BASE_URL = os.environ.get("FHIR_BASE_URL")
KF_FHIR_QA_OIDC_CLIENT_SECRET = os.environ.get("KF_FHIR_QA_OIDC_CLIENT_SECRET")


@dataclass
class FhirResourceType:
    """
    Wrapper class to define a FHIR resource type along with a global ID
    prefix
    """

    resource_type: str
    id_prefix: str


FHIR_RESOURCE_TYPES: dict = {
    resource_type: FhirResourceType(resource_type, prefix)
    for resource_type, prefix in [("DocumentReference", "dr")]
}


class SECRETS:
    """
    Used in logger initialization to obfuscate sensitive env variables
    """

    DEWRANGLE_DEV_PAT = "DEWRANGLE_DEV_PAT"
    CAVATICA_DEVELOPER_TOKEN = "CAVATICA_DEVELOPER_TOKEN"
    KF_FHIR_QA_OIDC_CLIENT_SECRET = "KF_FHIR_QA_OIDC_CLIENT_SECRET"
    DATASERVICE_DB_ADMIN_PW = "DATASERVICE_DB_ADMIN_PW"
    DATASERVICE_DB_ADMIN_PW = "DATASERVICE_DB_ADMIN_PW"
    POSTGRES_ADMIN_PW = "POSTGRES_ADMIN_PW"
    D3B_WAREHOUSE_DB_USER_PW = "D3B_WAREHOUSE_DB_USER_PW"
    D3B_WAREHOUSE_DB_ADMIN_PW = "D3B_WAREHOUSE_DB_ADMIN_PW"
    FHIR_APP_ADMIN_PW = "FHIR_APP_ADMIN_PW"
    AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
    AWS_ACCESS_KEY_SECRET = "AWS_ACCESS_KEY_SECRET"
    POSTGRES_ADMIN_USER = "POSTGRES_ADMIN_USER"
    POSTGRES_ADMIN_PW = "POSTGRES_ADMIN_PW"


class IdTypes(Enum):
    """
    Used in CLI option definitions
    """

    KIDS_FIRST = "kids_first"
    DEWRANGLE = "dewrangle"


class KidsFirstFhirEntity(Enum):
    """
    Names of Kids First clinical entities in FHIR. These map to FHIR
    resource types but more than one entity can map to the same FHIR resource
    type
    """

    # NOTE: Do not change order. These are in the order in which they must
    # be loaded into the server
    organization = "organization"
    practitioner = "practitioner"
    practitioner_role = "practitioner_role"
    patient = "patient"
    research_study = "research_study"
    research_subject = "research_subject"
    proband_status = "proband_status"
    family = "family"
    family_relationship = "family_relationship"
    sequencing_center = "sequencing_center"
    phenotype = "phenotype"
    disease = "disease"
    vital_status = "vital_status"
    parent_specimen = "parent_specimen"
    child_specimen = "child_specimen"
    histopathology = "histopathology"
    drs_document_reference = "drs_document_reference"
    drs_document_reference_index = "drs_document_reference_index"


class ImagingFhirEntity(Enum):
    """
    Names of imaging data entities in FHIR. These map to FHIR
    resource types but more than one entity can map to the same FHIR resource
    type
    """

    # NOTE: Do not change order. These are in the order in which they must
    # be loaded into the server
    imaging_device = "imaging_device"
    imaging_document_reference = "imaging_document_reference"
    imaging_study = "imaging_study"


IMAGING_ENTITY_TYPES = {
    "Patient": "imaging_patient",
    "Device": "imaging_device",
    "DocumentReference": "imaging_document_reference",
    "ImagingStudy": "imaging_study",
}
IMAGING_RESOURCE_TYPES = ["Device", "DocumentReference", "ImagingStudy"]

valid_kids_first_fhir_types = {et.value for et in KidsFirstFhirEntity}
valid_fhir_types = set(
    [et.value for et in KidsFirstFhirEntity]
    + [et.value for et in ImagingFhirEntity]
)

SKIP_ENTITIES = {"practitioner", "practitioner_role", "organization"}

ETL_STAGES = "etbl"


class TargetAPI(Enum):
    """
    Enum used in CLI option definitions
    """

    DEWRANGLE = "dewrangle"
    FHIR = "fhir"


def check_dewrangle_http_config():
    """
    Check if env vars are set
    """
    if not (DEWRANGLE_DEV_PAT and DEWRANGLE_BASE_URL):
        raise ValueError(
            "❌ Missing required configuration! Please set the environment"
            " variables: DEWRANGLE_BASE_URL with the url to Dewrangle, and"
            " DEWRANGLE_DEV_PAT with your personal access token on Dewrangle."
        )


config = {
    "logging": {
        "default_log_filename": "dwds_fhir_etl",
        "default_log_level": "info",
        "default_log_dir": LOG_DIR,
    },
    "fhir": {
        "base_url": FHIR_BASE_URL or "http://localhost:8000",
        "username": os.environ.get("FHIR_APP_ADMIN") or "admin",
        "password": os.environ.get("FHIR_APP_ADMIN_PW") or "password",
        "resource_types": {
            "Group": "gr",
            "Observation": "ob",
            "DocumentReference": "dr",
            "Specimen": "bs",
            "Condition": "cn",
            "ResearchSubject": "rs",
            "Patient": "pt",
            "PractitionerRole": "pr",
            "Organization": "or",
            "Practitioner": "pc",
            "ResearchStudy": "sd",
        },
        "mapping": {
            "organization": {"endpoint": "/Organization", "params": {}},
            "research_study": {"endpoint": "/ResearchStudy", "params": {}},
            "practitioner": {"endpoint": "/Practitioner", "params": {}},
            "practitioner_role": {
                "endpoint": "/PractitionerRole",
                "params": {},
            },
            "patient": {"endpoint": "/Patient", "params": {}},
            "research_subject": {"endpoint": "/ResearchSubject", "params": {}},
            "proband_status": {
                "endpoint": "/Observation",
                "params": {"code": "85900004"},
            },
            "family": {"endpoint": "/Group", "params": {"code": "FAMMEMB"}},
            "family_relationship": {
                "endpoint": "/Observation",
                "params": {"code": "FAMMEMB"},
            },
            "sequencing_center": {"endpoint": "/Organization", "params": {}},
            "phenotype": {
                "endpoint": "/Condition",
                "params": {
                    "_profile:below": "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/phenotype",
                },
            },
            "disease": {
                "endpoint": "/Condition",
                "params": {
                    "_profile:below": "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/disease",
                },
            },
            "vital_status": {
                "endpoint": "/Observation",
                "params": {"code": "263493007"},
            },
            "child_specimen": {"endpoint": "/Specimen", "params": {}},
            "parent_specimen": {"endpoint": "/Specimen", "params": {}},
            "histopathology": {
                "endpoint": "/Observation",
                "params": {"code": "250537006"},
            },
            "drs_document_reference": {
                "endpoint": "/DocumentReference",
                "params": {},
            },
            "drs_document_reference_index": {
                "endpoint": "/DocumentReference",
                "params": {},
            },
            "imaging_device": {
                "endpoint": "/Device",
                "params": {},
            },
            "imaging_document_reference": {
                "endpoint": "/DocumentReference",
                "params": {},
            },
            "imaging_study": {
                "endpoint": "/ImagingStudy",
                "params": {},
            },
        },
    },
    "dewrangle": {
        "base_url": DEWRANGLE_BASE_URL,
        "pagination": {"max_page_size": DEWRANGLE_MAX_PAGE_SIZE},
        "endpoints": {
            "graphql": "/api/graphql",
            "rest": {
                "study_file": "api/rest/studies/{dewrangle_study_id}/files/{filename}",
                "global_id": "api/rest/studies/{dewrangle_study_id}/global-descriptors",
                "job_errors": "api/rest/jobs/{job_id}/errors",
            },
        },
        "ingest": [e.value for e in KidsFirstFhirEntity],
    },
    "dataservice": {
        "api_url": os.environ.get("DATASERVICE_BASE_URL")
        or "http://localhost:5000",
        "prefix": {
            "SequencingCenter": "SC",
            "Investigator": "IG",
            "Study": "SD",
            "Family": "FM",
            "FamilyRelationship": "FR",
            "Participant": "PT",
            "Phenotype": "PH",
            "Diagnosis": "DG",
            "Outcome": "OC",
            "Biospecimen": "BS",
            "Sample": "SA",
            "SampleRelationship": "SR",
            "GenomicFile": "GF",
            "SequencingExperiment": "SE",
            "SequencingExperimentGenomicFile": "SG",
            "BiospecimenGenomicFile": "BG",
            "BiospecimenDiagnosis": "BD",
        },
        "seeder": {
            "SequencingCenter": {"total": 1},
            "Investigator": {"total": 1},
            "Study": {
                "foreign_keys": ["Investigator"],
                "total": 2,  # total study entities
            },
            "Family": {"total": 3},
            "Participant": {
                "foreign_keys": [
                    "Family",
                    "Study",
                ],
                "total": 10,
            },
            # Family relationship will be manually
            # created. Therefore no need to specify fk
            "FamilyRelationship": {"total": 3},
            "Phenotype": {"foreign_keys": ["Participant"], "total": 20},
            "Diagnosis": {"foreign_keys": ["Participant"], "total": 20},
            "Outcome": {"foreign_keys": ["Participant"], "total": 20},
            "Sample": {
                "foreign_keys": ["Participant"],
                "total": 40,
            },
            # Sample relationship will be manually
            # created. Therefore no need to specify fk
            "SampleRelationship": {"total": 20},
            "Biospecimen": {
                "foreign_keys": ["SequencingCenter", "Participant", "Sample"],
                "total": 40,
            },
            "GenomicFile": {"total": 40},
            "BiospecimenGenomicFile": {
                "foreign_keys": [
                    "Biospecimen",
                    "GenomicFile",
                ],
                "total": 40,
            },
            "BiospecimenDiagnosis": {
                "foreign_keys": [
                    "Biospecimen",
                    "Diagnosis",
                ],
                "total": 40,
            },
            "SequencingExperiment": {
                "foreign_keys": [
                    "SequencingCenter",
                ],
                "total": 40,
            },
            "SequencingExperimentGenomicFile": {
                "foreign_keys": [
                    "SequencingExperiment",
                    "GenomicFile",
                ],
                "total": 40,
            },
        },
        # Must be in the order that satisfies foreign key relationships
        "endpoints": {
            "SequencingCenter": "/sequencing-centers",
            "Investigator": "/investigators",
            "Study": "/studies",
            "Family": "/families",
            "Participant": "/participants",
            "FamilyRelationship": "/family-relationships",
            "Sample": "/samples",
            "Biospecimen": "/biospecimens",
            "SampleRelationship": "/sample-relationships",
            "Diagnosis": "/diagnoses",
            "Phenotype": "/phenotypes",
            "Outcome": "/outcomes",
            "GenomicFile": "/genomic-files",
            "BiospecimenGenomicFile": "/biospecimen-genomic-files",
            "BiospecimenDiagnosis": "/biospecimen-diagnoses",
            "ReadGroup": "/read-groups",
            "SequencingExperiment": "/sequencing-experiments",
            "ReadGroupGenomicFile": "/read-group-genomic-files",
            "SequencingExperimentGenomicFile": "/sequencing-experiment-genomic-files",
        },
        "cached_schema_filepath": os.path.join(ROOT_DIR, "cached_schema.json"),
    },
    "faker": {"global_id": {"fhir_resource_types": FHIR_RESOURCE_TYPES}},
}
