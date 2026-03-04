"""
GraphQL methods to CRUD organization in Dewrangle
"""

import os
import logging
from pprint import pformat

from d3b_api_client_cli.dewrangle.graphql.common import exec_query
from d3b_api_client_cli.dewrangle.graphql.job import (
    queries,
    mutations,
)
from d3b_api_client_cli.config import DEWRANGLE_DIR
from d3b_api_client_cli.utils import write_json

logger = logging.getLogger(__name__)


def read_fhir_ingest_job(node_id, output_dir=DEWRANGLE_DIR):
    """
    Fetch Job by ID from Dewrangle

    :param node_id: Dewrangle node ID of the job
    :type node_id: str
    :rtype: dict
    :returns: the job
    """
    params = {"id": node_id}

    resp = exec_query(queries.fhir_resource_ingest_job, variables=params)

    result = resp["node"]
    operation = result["operation"].lower().replace("_", "-")
    errors = result["errors"]["edges"]
    logger.info(f"Fetched job {result['id']}")
    if errors:
        logger.warning(f"‼️  Read job {operation} failed:\n{pformat(errors)}")
    else:
        logger.info(f"🚦 Job-{operation}:\n{pformat(result)}")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"Job-{operation}.json")
        write_json(result, filepath)
        emoji = "‼️ " if errors else "✅"
        logger.info(
            f"✏️  Wrote {operation} job to {filepath}. {emoji}  Found"
            f" {len(errors)} errors"
        )

    return result


def fhir_resource_ingest(dewrangle_study_id, dewrangle_file_ids):
    """
    Start a Dewrangle job to ingest previously uploaded study files
    containing FHIR resources.

    :param dewrangle_study_id: GraphQL ID of the Dewrangle study.
    :type dewrangle_study_id: str

    :param dewrangle_file_ids: List of Dewrangle StudyFile input objects
        to ingest. Each item must match the GraphQL type
        `FhirResourceIngestFileInput`, typically:
            [{"id": "<study_file_graphql_id>"}]
    :type dewrangle_file_ids: list[dict]

    :rtype: dict
    :returns: Raw GraphQL response from the `fhirResourceIngest` mutation.
    """
    params = {
        "input": {
            "studyId": dewrangle_study_id,
            "studyFileIds": dewrangle_file_ids,
        }
    }
    resp = exec_query(mutations.fhir_resource_ingest, variables=params)

    return resp
