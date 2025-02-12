"""
Dewrangle functions to create, update, remove global descriptors in Dewrangle 
"""

from enum import Enum
from typing import Optional
from pprint import pformat
import logging
import os

from d3b_api_client_cli.dewrangle.graphql import study as study_api
from d3b_api_client_cli.dewrangle.rest.files import download_file

from d3b_api_client_cli.config import (
    config,
    ROOT_DATA_DIR
)
from d3b_api_client_cli.dewrangle.rest import (
    upload_study_file,
)
from d3b_api_client_cli.utils import timestamp

logger = logging.getLogger(__name__)

CSV_CONTENT_TYPE = "text/csv"
DEWRANGLE_BASE_URL = config["dewrangle"]["base_url"].rstrip("/")
DEFAULT_FILENAME = f"dewrangle-file-{timestamp()}.csv"


class GlobalIdDescriptorOptions(Enum):
    """
    Used in download_global_descriptors 
    """
    DOWNLOAD_ALL_DESC = "all"
    DOWNLOAD_MOST_RECENT = "most-recent"


def upsert_and_download_global_descriptors(
    input_filepath: str,
    study_global_id: Optional[str],
    dewrangle_study_id: Optional[str],
    skip_unavailable_descriptors: Optional[bool] = True,
    descriptors: Optional[GlobalIdDescriptorOptions] = GlobalIdDescriptorOptions.DOWNLOAD_ALL_DESC.value,  # noqa
    output_dir: Optional[str] = None,
    output_filepath: Optional[str] = None,
) -> str:
    """
    Send request to upsert global descriptors and download created/updated 
    global descriptors and ID from Dewrangle

    Args:
        See upsert_global_descriptors and
        d3b_api_client_cli.dewrangle.rest.download_global_descriptors

    Options:
        See upsert_global_descriptors and
        d3b_api_client_cli.dewrangle.rest.download_global_descriptors

    Returns:
        filepath: path to downloaded global ID descriptors
    """
    if not output_dir:
        output_dir = os.path.join(ROOT_DATA_DIR)
        os.makedirs(output_dir, exist_ok=True)

    result = upsert_global_descriptors(
        input_filepath,
        study_global_id=study_global_id,
        dewrangle_study_id=dewrangle_study_id,
        skip_unavailable_descriptors=skip_unavailable_descriptors,
    )

    job_id = result["job"]["id"]
    dewrangle_study_id = result["study_id"]

    filepath = download_global_descriptors(
        dewrangle_study_id=dewrangle_study_id,
        job_id=job_id,
        descriptors=descriptors,
        filepath=output_filepath,
        output_dir=output_dir,
    )

    return filepath


def upsert_global_descriptors(
    filepath: str,
    study_global_id: Optional[str],
    dewrangle_study_id: Optional[str],
    skip_unavailable_descriptors: Optional[bool] = True,
):
    """
    Upsert global descriptors to Dewrangle

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

    result = upload_study_file(dewrangle_study_id, filepath=filepath)
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
        "✅ Completed request to upsert global descriptors. Job ID: %s",
        job_id
    )

    return result


def download_global_descriptors(
    dewrangle_study_id: Optional[str] = None,
    study_global_id: Optional[str] = None,
    job_id: Optional[str] = None,
    descriptors: Optional[GlobalIdDescriptorOptions] = None,  # noqa
    filepath: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """
    Download study's global IDs from Dewrangle

    Args:
        - dewrangle_study_id: GraphQL ID of study in Dewrangle
        - filepath: GraphQL ID of study in Dewrangle
    Options:
        - job_id: The job ID returned from the upsert_global_descriptors 
                  method. If this is provided, only global IDs from that
                  job will be returned.

        - descriptors: A query parameter that determines how many descriptors 
                       will be returned for the global ID. 

                       If set to "all" return all descriptors associated 
                       with the global ID

                       If set to "most-recent" return the most recent
                       descriptor associated with the global ID

        - filepath: If filepath is provided, download content to that filepath

        - output_dir: If output_dir is provided, get filename from
                      Content-Disposition header and download the file to the
                      output directory with that filename
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

    if not descriptors:
        descriptors = GlobalIdDescriptorOptions.DOWNLOAD_ALL_DESC.value

    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["global_id"]
    endpoint = endpoint_template.format(dewrangle_study_id=dewrangle_study_id)
    url = f"{base_url}/{endpoint}"

    params = {}
    if job_id:
        params.update({"job": job_id})
    if descriptors:
        params.update({"descriptors": descriptors})

    logger.info(
        "🛸 Start download of global IDs for study %s from Dewrangle: %s"
        " Params: %s",
        study_global_id,
        url,
        pformat(params)
    )

    filepath = download_file(
        url,
        output_dir=output_dir,
        filepath=filepath,
        params=params
    )

    logger.info("✅ Completed download of global IDs: %s", filepath)

    return filepath
