"""
Poll Dewrangle for status on various jobs:
    - FHIR ingest job
"""

import time
import logging

from pprint import pformat
from d3b_api_client_cli.dewrangle.graphql.common import exec_query
from d3b_api_client_cli.dewrangle.graphql.job import (
    queries,
)

logger = logging.getLogger(__name__)

DEFAULT_INGEST_TIMEOUT_SEC = 3600
DEFAULT_UPSERT_TIMEOUT_SEC = 3600
DEFAULT_POLL_INTERVAL = 2


def is_complete(resp):
    complete = resp["node"]["completedAt"] is not None
    success = not resp["node"]["errors"]["edges"]

    return {"complete": complete, "success": success}


def poll_fhir_ingest_job(job_id, timeout_seconds=DEFAULT_INGEST_TIMEOUT_SEC):
    """
    Poll for status on a Dewrangle FHIR ingest job

    See _poll_job for details
    """
    job_query = queries.fhir_resource_ingest_job

    return _poll_job(
        job_id, job_query, is_complete, timeout_seconds=timeout_seconds
    )


def poll_descriptor_upsert_job(
    job_id, timeout_seconds=DEFAULT_UPSERT_TIMEOUT_SEC
):
    """
    Poll for status on a Dewrangle descriptor upsert job

    See _poll_job for details
    """
    job_query = queries.job_status_query

    return _poll_job(
        job_id, job_query, is_complete, timeout_seconds=timeout_seconds
    )


def _validate_status_format(status):
    for key in ["complete", "success"]:
        if key not in status:
            raise Exception(
                "Invalid poll job complete function. Must return a dict "
                "with the following format: {'complete': <boolean\n>, "
                "'success': <boolean>}"
            )


def _poll_job(
    job_id,
    job_query,
    complete_function,
    timeout_seconds=None,
    interval_seconds=DEFAULT_POLL_INTERVAL,
):
    """
    Poll for status on a Dewrangle job. If timeout is not set poll until job
    is complete. If timeout is set, poll until job is complete or timeout
    expires

    Return True if job is complete without errors
    Return False if job is complete with errors
    Return False if timeout is exceeded and job is not complete

    :param node_id: Dewrangle node ID of the job
    :type node_id: str
    :param job_query: A GraphQL query to fetch the job
    :type job_query: gql
    :param complete_function: A method which determines when the job is
    complete and if it succeeded. This method will take in a dict containing
    the output of the graphql query and it must return a dict containing the
    following: { "complete": boolean, "success": boolean }
    :type complete_function: A Python function
    """
    elapsed_time_seconds = 0
    start_time = time.time()

    while True:
        # Fetch job
        params = {"id": job_id}
        resp = exec_query(job_query, variables=params)

        job = resp["node"]
        node_id = job["id"]
        operation = job["operation"].lower().replace("_", "-")

        # Check completion status
        status = complete_function(resp)
        _validate_status_format(status)

        # Job completed
        if status["complete"]:
            success = status["success"]
            emoji = "✅ " if success else "‼️ "
            suffix = "" if success else " with errors"
            logger.info(
                f"{emoji} Job {operation} {node_id}"
                f" completed{suffix}:\n{pformat(job)}"
            )

            return {"status": status["complete"], "job": job}

        elapsed_time_seconds = time.time() - start_time
        print(elapsed_time_seconds, timeout_seconds)
        t = time.strftime("%H:%M:%S", time.gmtime(elapsed_time_seconds))

        # Timeout exceeded
        if (timeout_seconds is not None) and (
            elapsed_time_seconds > timeout_seconds
        ):
            logger.warning(
                f"⚠️  Timeout of {timeout_seconds} seconds expired."
                f" Current job {operation} {node_id} result:\n{pformat(job)}"
                f"\n✌️ Dewrangle must still be working ... but CLI is moving "
                "on"
            )
            return {"status": False, "job": job}

        # Continue polling
        logger.info(
            f"⏰ Waiting for job {operation} {node_id} to"
            f" complete. Elapsed time (hh:mm:ss): {t}"
        )

        time.sleep(interval_seconds)
