"""
Dewrangle functions to download files from the REST API 
"""

from typing import Optional
from enum import Enum
from pprint import pformat
import logging
import os
import cgi

from d3b_api_client_cli.dewrangle.graphql import study as study_api

from d3b_api_client_cli.config import (
    DEWRANGLE_DEV_PAT,
    config,
    check_dewrangle_http_config,
    ROOT_DATA_DIR
)
from d3b_api_client_cli.utils import send_request, timestamp

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


def _filename_from_headers(headers: dict) -> str:
    """
    Helper to get the filename from the Content-Disposition
    header of an HTTP response
    """
    _, params = cgi.parse_header(headers["Content-Disposition"])
    return params.get("filename")


def upload_file(
    url: str,
    filepath: Optional[str] = None,
    params: Optional[dict] = None
):
    """
    Upload a file to Dewrangle
    """
    logger.info("🛸 Starting upload of %s to %s", filepath, url)
    with open(filepath, "rb") as jsonfile:
        headers = {"x-api-key": DEWRANGLE_DEV_PAT}
        resp = send_request(
            "post",
            url,
            headers=headers,
            data=jsonfile,
            params=params,
            # Set timeout to infinity so that uploads don't timeout
            timeout=-1,
        )

    logger.info("✅ Completed upload: %s", os.path.split(filepath)[-1])
    logger.info(pformat(resp.json()))

    return resp.json()


def download_file(
    url: str,
    output_dir: Optional[str] = None,
    filepath: Optional[str] = None,
    params: Optional[dict] = None
) -> str:
    """
    Download a file from Dewrangle

    If filepath is provided, download content to that filepath

    If output_dir is provided, get filename from Content-Disposition header
    and download the file to the output directory with that filename

    Return:
        filepath - if the downloaded file was not empty
        None - if the downloaded file was empty
    """
    logger.info("🛸 Start downloading file from Dewrangle %s ...", url)

    if (not filepath) and (not output_dir):
        output_dir = os.path.join(ROOT_DATA_DIR)
        os.makedirs(output_dir, exist_ok=True)

    headers = {"x-api-key": DEWRANGLE_DEV_PAT,
               "content-type": CSV_CONTENT_TYPE}
    resp = send_request(
        "get",
        url,
        params=params,
        headers=headers,
    )
    if not filepath:
        filename = _filename_from_headers(resp.headers)
        if not filename:
            filename = DEFAULT_FILENAME
        filepath = os.path.join(output_dir, filename)

    with open(filepath, "wb") as out_file:
        out_file.write(resp.content)

    logger.info("Download from %s", resp.url)
    logger.info("✅ Completed download file: %s", filepath)

    return filepath


def upload_study_file(dewrangle_study_id: str, filepath: str):
    """
    Upload a CSV file to Dewrangle's study file endpoint
    """
    filepath = os.path.abspath(filepath)
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["study_file"]
    endpoint = endpoint_template.format(
        dewrangle_study_id=dewrangle_study_id,
        filename=os.path.split(filepath)[-1],
    )
    url = f"{base_url}/{endpoint}"

    return upload_file(url)


def download_global_descriptors(
    dewrangle_study_id: str,
    job_id: Optional[str] = None,
    descriptors: Optional[GlobalIdDescriptorOptions] = GlobalIdDescriptorOptions.DOWNLOAD_ALL_DESC.value,  # noqa
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
    study = study_api.read_study(dewrangle_study_id)

    if not study:
        raise ValueError(
            f"❌ Study {dewrangle_study_id}"
            " does not exist in Dewrangle. Aborting"
        )

    study_global_id = study["globalId"]

    logger.info(
        "🛸 Start downloading global IDs for study %s from Dewrangle ...",
        study_global_id
    )

    filepath = os.path.abspath(filepath)
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["global_id"]
    endpoint = endpoint_template.format(dewrangle_study_id=dewrangle_study_id)
    url = f"{base_url}/{endpoint}"

    params = {}
    if job_id:
        params.update({"job": job_id})
    if descriptors:
        params.update({"descriptors": descriptors})

    filepath = download_file(
        url,
        output_dir=output_dir,
        filepath=filepath,
        params=params
    )

    logger.info("✅ Completed download of global IDs: %s", filepath)

    return filepath


def download_job_errors(
    job_id: str,
    output_dir: Optional[str] = None,
    filepath: Optional[str] = None,
) -> str:
    """
    Download a job's error report from Dewrangle

    See download_file for details
    """
    # Ensure env vars are set
    check_dewrangle_http_config()

    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["job_errors"]
    endpoint = endpoint_template.format(job_id=job_id)
    url = f"{DEWRANGLE_BASE_URL}{endpoint}"

    return download_file(url, filepath=filepath, output_dir=output_dir)
