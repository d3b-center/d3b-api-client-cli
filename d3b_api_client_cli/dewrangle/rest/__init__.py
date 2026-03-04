"""
Upload study files: POST api/rest/studies/<study_id>/files/<filename>

Study files contain JSON formatted lists of FHIR resources

https://stackoverflow.com/questions/72911304/how-to-upload-large-files-using-post-method-in-python
"""

import os
import logging
from pprint import pprint, pformat

from d3b_api_client_cli.config import DEWRANGLE_DEV_PAT, config
from d3b_api_client_cli.utils import send_request
from d3b_api_client_cli.dewrangle.graphql.study import (
    get_study_by_kf_id,
    upsert_global_descriptors,
)
from d3b_api_client_cli.dewrangle.rest.files import *

logger = logging.getLogger(__name__)

JSON_CONTENT_TYPE = "application/json"
CSV_CONTENT_TYPE = "text/csv"


def upload_study_file(dewrangle_study_id, filepath):
    """
    Upload a CSV file to Dewrangle's study file endpoint

    :param dewrangle_study_id: the ID of the study in Dewrangle
    :type dewrangle_study_id: str
    :param filepath: the path of the file to upload to Dewrangle
    :type filepath: str
    """
    filepath = os.path.abspath(filepath)
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["study_file"]
    endpoint = endpoint_template.format(
        dewrangle_study_id=dewrangle_study_id,
        filename=os.path.split(filepath)[-1],
    )
    url = f"{base_url}/{endpoint}"

    logger.info(f"⏰ Starting upload of {filepath} ...")
    with open(filepath, "rb") as jsonfile:
        headers = {"x-api-key": DEWRANGLE_DEV_PAT}
        resp = send_request(
            "post",
            url,
            headers=headers,
            data=jsonfile,
            # Set timeout to infinity so that uploads don't timeout
            timeout=-1,
        )

    logger.info(f"✅ Completed upload: {os.path.split(filepath)[-1]}")
    logger.info(pformat(resp.json()))

    return resp.json()


def request_global_ids(
    kf_study_id,
    filepath,
    content_type=CSV_CONTENT_TYPE,
    skip_unavailable_descriptors=True,
):
    """
    Request global IDs from Dewrangle for the given FHIR resources

    This happens in two steps:
        1. Upload the global descriptor csv file to the study file endpoint
        2. Invoke the graphQL mutation to upsert global descriptors

    :param kf_study_id: Kids First ID of the study in Dataservice
    :type kf_study_id: str
    :param filepath: path to the global descriptors csv file
    :type fileapth: str
    :param content_type: value of the Content-Type head to use in the upload
    file request
    :type content_type: str
    :param skip_unavailable_descriptors: If true any errors due to a descriptor
    already having a global ID assigned will be ignored
    :type skip_unavailable_descriptors: boolean
    """
    logger.info(f"🛸 Fetch global IDs from Dewrangle for {kf_study_id} ...")
    dewrangle_study = get_study_by_kf_id(kf_study_id)
    if not dewrangle_study:
        raise Exception(
            f"‼️  Study {kf_study_id} does not exist in Dewrangle."
            " Please run the 'dwds dewrangle setup-dewrangle-study' command"
            " to create and setup your study in Dewrangle"
        )
    dewrangle_study_id = dewrangle_study["id"]

    filepath = os.path.abspath(filepath)
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["study_file"]
    endpoint = endpoint_template.format(
        dewrangle_study_id=dewrangle_study_id,
        filename=os.path.split(filepath)[-1],
    )

    logger.info(f"🛸 POST global IDs request file {filepath} to Dewrangle")
    url = f"{base_url}/{endpoint}"
    with open(filepath, "rb") as request_file:
        headers = {
            "x-api-key": DEWRANGLE_DEV_PAT,
            "Content-Type": content_type,
        }
        resp = send_request(
            "post",
            url,
            headers=headers,
            data=request_file,
        )
    result = resp.json()
    study_file_id = result["id"]

    # Trigger global descriptor upsert mutation
    resp = upsert_global_descriptors(
        dewrangle_study_id,
        study_file_id,
        skip_unavailable_descriptors=skip_unavailable_descriptors,
    )
    result = resp["globalDescriptorUpsert"]
    job_id = result["job"]["id"]

    logger.info(
        f"✅ Completed request to generate global IDs. Job ID: {job_id}"
    )

    return result


def download_global_ids(kf_study_id, filepath, job_id=None, descriptors="all"):
    """
    Download study's global IDs from Dewrangle
    """
    logger.info(f"🛸 Get dewrangle study id for {kf_study_id} ...")
    dewrangle_study = get_study_by_kf_id(kf_study_id)
    if not dewrangle_study:
        raise Exception(
            f"‼️  Study {kf_study_id} does not exist in Dewrangle."
            " Please run the 'dwds dewrangle setup-dewrangle-study' command"
            " to create and setup your study in Dewrangle"
        )
    dewrangle_study_id = dewrangle_study["id"]

    filepath = os.path.abspath(filepath)
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["global_id"]
    endpoint = endpoint_template.format(dewrangle_study_id=dewrangle_study_id)
    base = base_url.rstrip("/")
    path = endpoint.lstrip("/")
    url = f"{base}/{path}"

    logger.info("🛸 Start downloading global IDs from Dewrangle ...")

    params = {}
    if job_id:
        params.update({"job": job_id})
    if descriptors:
        params.update({"descriptors": descriptors})

    with open(filepath, "wb") as csvfile:
        headers = {
            "x-api-key": DEWRANGLE_DEV_PAT,
        }
        resp = send_request(
            "get",
            url,
            headers=headers,
            params=params,
            # Set timeout to infinity so that downloads don't timeout
            timeout=-1,
        )
        csvfile.write(resp.content)

    logger.info(f"✅ Completed download of generate global IDs {filepath}")
    logger.info(resp.url)

    return filepath


def download_job_errors(job_id, filepath):
    """
    Download study's global IDs from Dewrangle

    :param job_id: A Dewrangle generated ID of the job that has errors
    :type job_id: str
    :param filepath: Path to the file where errors will be written
    :type filepath: str
    """
    base_url = config["dewrangle"]["base_url"]
    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["job_errors"]
    endpoint = endpoint_template.format(job_id=job_id)
    url = f"{base_url}/{endpoint}"

    logger.info("🛸 Start downloading job {job_id} errors from Dewrangle ...")

    with open(filepath, "wb") as csvfile:
        headers = {
            "x-api-key": DEWRANGLE_DEV_PAT,
        }
        resp = send_request(
            "get",
            url,
            headers=headers,
            # Set timeout to infinity so that downloads don't timeout
            timeout=-1,
        )
        csvfile.write(resp.content)

    logger.info(f"✅ Completed download job errors: {filepath}")
    logger.info(resp.url)

    return filepath
