"""
Dewrangle functions to create, update, remove global descriptors in Dewrangle 
"""

from typing import Optional
import logging
import os

from d3b_api_client_cli.dewrangle.graphql import study as study_api

from d3b_api_client_cli.config import (
    config,
)
from d3b_api_client_cli.dewrangle.rest import upload_study_file
from d3b_api_client_cli.utils import timestamp

logger = logging.getLogger(__name__)

CSV_CONTENT_TYPE = "text/csv"
DEWRANGLE_BASE_URL = config["dewrangle"]["base_url"].rstrip("/")
DEFAULT_FILENAME = f"dewrangle-file-{timestamp()}.csv"


def upsert_global_descriptors(
    filepath: str,
    study_global_id: Optional[str],
    dewrangle_study_id: Optional[str],
    skip_unavailable_descriptors: Optional[bool] = True,
):
    """
    Upsert global IDs to Dewrangle

    This happens in two steps:
        1. Upload the global descriptor csv file to the study file endpoint
        2. Invoke the graphQL mutation to upsert global descriptors

    Args:
     - skip_unavailable_descriptors (bool): If true any errors due to a
     descriptor already having a global ID assigned will be ignored

    Options:
      - study_global_id - Provide this when you don't know the study's 
      GraphQL ID in Dewrangle.
      - study_id - Study GraphQL ID in Dewrangle

      You must provide either the study_global_id OR the study_id but not both 

    Raise:
        ValueError if the study does not exist in Dewrangle
    """
    if dewrangle_study_id:
        study = study_api.read_study(dewrangle_study_id)
    else:
        study = study_api.find_study(study_global_id)

    if not study:
        raise ValueError(
            f"❌ Study "
            f"{study_global_id if study_global_id else dewrangle_study_id}"
            " does not exist in Dewrangle. Aborting"
        )

    study_global_id = study["globalId"]
    dewrangle_study_id = study["id"]

    logger.info(
        "🛸 Upsert global IDs in %s to Dewrangle for study %s",
        filepath, study_global_id
    )

    filepath = os.path.abspath(filepath)
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["study_file"]
    endpoint = endpoint_template.format(
        dewrangle_study_id=dewrangle_study_id,
        filename=os.path.split(filepath)[-1],
    )

    url = f"{base_url}/{endpoint}"
    logger.info("🛸 POST global IDs file %s to Dewrangle %s", filepath, url)

    result = upload_study_file(dewrangle_study_id, filepath)
    study_file_id = result["id"]

    # Trigger global descriptor upsert mutation
    resp = study_api.upsert_global_descriptors(
        study_file_id,
        skip_unavailable_descriptors=skip_unavailable_descriptors
    )
    result = resp["globalDescriptorUpsert"]
    job_id = result["job"]["id"]
    result["study_global_id"] = study_global_id
    result["study_id"] = study["id"]

    logger.info(
        "✅ Completed request to upsert global IDs. Job ID: %s", job_id)

    return result
