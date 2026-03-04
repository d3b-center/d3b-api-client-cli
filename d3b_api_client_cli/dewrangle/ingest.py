"""
Ingest FHIR data into Dewrangle

Single File Ingest
------------------
- Upload study file containing FHIR resources to Dewrangle study files endpoint
- Start Dewrangle FHIR resource ingest job for the file

Study Ingest
------------
- Start job to ingest all study files into Dewrangle given a KF study ID
"""

import os
import logging
from pprint import pformat

from d3b_api_client_cli.utils import read_json, kf_id_to_global_id
from d3b_api_client_cli.config import (
    config,
    KidsFirstFhirEntity,
    SKIP_ENTITIES,
)
from d3b_api_client_cli.dewrangle import graphql as gql_client
from d3b_api_client_cli.dewrangle.graphql.study import (
    get_study_by_kf_id,
)
from d3b_api_client_cli.dewrangle.rest import upload_study_file
from d3b_api_client_cli.dewrangle.poll_job import poll_fhir_ingest_job

logger = logging.getLogger(__name__)
config = config["dewrangle"]
valid_kids_first_fhir_types = set([et.value for et in KidsFirstFhirEntity])


def upload_and_ingest_study_file(kf_study_id, filepath):
    """
    Upload study file to Dewrangle and start FHIR resource ingest job

    * For dev and debugging purposes *

    :param kf_study_id: the ID of the study in Dewrangle
    :type kf_study_id: str
    :param filepath: the path of the file to ingest into Dewrangle
    :type filepath: str

    :rtype: dict
    :returns: GraphQL response from ingest mutation
    """
    # Upload study file to Dewrangle study
    dewrangle_study = get_study_by_kf_id(kf_study_id)
    if not dewrangle_study:
        logger.warning(
            f"⚠️  Could not ingest study file. Failed to find corresponding "
            f" dewrangle study ID for KF ID {kf_study_id}"
        )
        return

    dewrangle_study_id = dewrangle_study["id"]
    resp = upload_study_file(dewrangle_study_id, filepath)
    logger.info(f"🛸 Uploaded {filepath}:\n{pformat(resp)}")

    # Submit request to ingest FHIR resource files into Dewrangle
    to_ingest = []
    to_ingest.append({"id": resp["id"]})
    resp = gql_client.fhir_resource_ingest(dewrangle_study_id, to_ingest)
    logger.info(
        f"✅ Submitted request to start FHIR resource ingest for"
        f" {filepath}:\n{pformat(resp)}"
    )
    # Check on job status
    job = resp["fhirResourceIngest"]
    errors = job.get("errors")
    if errors:
        logger.warning(f"⚠️  Dewrangle error:\n{pformat(errors)}")
        return resp

    # Return job status
    node_id = job["job"]["id"]
    result = poll_fhir_ingest_job(node_id)

    return result


def ingest_study_files(
    study_data_dir_or_file, dewrangle_study_node_id=None, entities_to_load=None
):
    """
    Start job to ingest a study's FHIR resource files into Dewrangle

    - Get study in Dewrangle either by KF ID lookup or direct lookup via
    the graphql node id `dewrangle_study_id`
    - Start job to ingest study's FHIR resource files into Dewrangle

    :param study_data_dir_or_file: the path to the filr or dir of files to ingest
    :type study_data_dir_or_file: str
    :param dewrangle_study_id: the GraphQL node ID of the Dewrangle study
    :type dewrangle_study_id: str

    :rtype: dict
    :returns: resp from start FHIR resource ingest mutation
    """
    if not entities_to_load:
        entities_to_load = valid_kids_first_fhir_types
    else:
        entities_to_load = set(entities_to_load)

    # NOTE - This is temporary until Dewrangle is able to support these having
    # these types in multiple studies
    entities_to_load = entities_to_load - SKIP_ENTITIES

    # Determine if file or dir
    ingest_one_file = False
    study_data_dir = study_data_dir_or_file
    if os.path.isfile(study_data_dir_or_file):
        study_data_dir = os.path.dirname(study_data_dir_or_file)
        ingest_one_file = True

    # Get study from in Dewrangle
    study_filepath = os.path.join(study_data_dir, "Study.json")
    study_params = read_json(study_filepath)
    kf_study_id = study_params["kf_id"]

    if dewrangle_study_node_id:
        dewrangle_study = gql_client.read_study(dewrangle_study_node_id)
    else:
        dewrangle_study = get_study_by_kf_id(kf_study_id)

    if not dewrangle_study:
        logger.error(
            f"‼️  Study {kf_study_id} does not exist in Dewrangle."
            " Please run the 'dwds dewrangle setup-dewrangle-study' command"
            " to create and setup your study in Dewrangle"
        )
        return
    dewrangle_study_id = dewrangle_study["id"]

    # Configure file upload endpoint
    base_url = config["base_url"]
    endpoint = config["endpoints"]["rest"]["study_file"]
    logger.info(f"🛸 Starting upload of study files to {base_url}/{endpoint}")

    # Upload FHIR resource files
    entities = config["ingest"]
    to_ingest = []
    for kf_entity in entities:
        if kf_entity not in entities_to_load:
            logger.info(
                f"⏭️  Skip {kf_entity}. User did not include it in"
                "entities_to_load"
            )
            continue

        if kf_entity == "sequencing_center":
            continue

        filepath = os.path.join(study_data_dir, f"{kf_entity}.json")
        if ingest_one_file and (filepath != study_data_dir_or_file):
            continue

        if not os.path.exists(filepath):
            logger.info(f"ℹ️  Skipping ingest of {kf_entity}. No data exists")
            continue

        resp = upload_study_file(dewrangle_study_id, filepath)
        to_ingest.append({"id": resp["id"]})

    # Submit request to ingest FHIR resource files into Dewrangle
    resp = gql_client.fhir_resource_ingest(dewrangle_study_id, to_ingest)
    logger.info(
        f"✅ Submitted request to start FHIR resource ingest for"
        f" {study_data_dir_or_file}:\n{pformat(resp)}"
    )
    # Check on job status
    job = resp["fhirResourceIngest"]
    errors = job.get("errors")
    if errors:
        logger.warning(f"⚠️  Dewrangle error:\n{pformat(errors)}")
        return resp

    # Return job status
    node_id = job["job"]["id"]
    result = poll_fhir_ingest_job(node_id)

    return result
