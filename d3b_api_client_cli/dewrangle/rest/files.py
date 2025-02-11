"""
Dewrangle functions to download files from the REST API 
"""

from typing import Optional
from pprint import pformat
import logging
import os
import cgi

from d3b_api_client_cli.dewrangle.graphql import study as study_api

from d3b_api_client_cli.config import (
    DEWRANGLE_DEV_PAT,
    config,
    check_dewrangle_http_config,
)
from d3b_api_client_cli.utils import send_request, timestamp

logger = logging.getLogger(__name__)

CSV_CONTENT_TYPE = "text/csv"
DEWRANGLE_BASE_URL = config["dewrangle"]["base_url"].rstrip("/")
DEFAULT_FILENAME = f"dewrangle-file-{timestamp()}.csv"


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
