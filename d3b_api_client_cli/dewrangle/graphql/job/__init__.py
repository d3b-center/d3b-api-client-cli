"""
GraphQL methods for jobs in Dewrangle
"""

import time
import os
import logging
from pprint import pformat
from typing import Callable, Optional

from graphql import DocumentNode

from d3b_api_client_cli.dewrangle.graphql.common import exec_query
from d3b_api_client_cli.dewrangle.graphql.job import (
    queries,
    mutations,
)
from d3b_api_client_cli.config import config
from d3b_api_client_cli.utils import (
    write_json,
)

logger = logging.getLogger(__name__)

DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
DEWRANGLE_MAX_PAGE_SIZE = config["dewrangle"]["pagination"]["max_page_size"]


def poll_job(
    job_id: str,
    timeout_seconds: Optional[int] = None,
    interval_seconds: Optional[int] = 30,
):
    """
    Poll for status on a Dewrangle FHIR ingest job

    See _poll_job for details
    """
    job_query = queries.job

    def is_complete(resp):
        complete = resp["node"]["completedAt"] is not None
        success = not resp["node"]["errors"]["edges"]

        return {"complete": complete, "success": success}

    return _poll_job(
        job_id,
        job_query,
        is_complete,
        timeout_seconds=timeout_seconds,
        interval_seconds=interval_seconds,
    )


def _validate_status_format(status: dict):
    """
    Validate that the deveoper supplied a properly formatted function for
    job completion
    """
    expected = {"complete": "<boolean>", "success": "<boolean>"}
    for key in ["complete", "success"]:
        if key not in status:
            raise ValueError(
                "❌ Invalid poll job complete function. Must return a dict "
                f"with the following format: {pformat(expected)}"
            )


def _poll_job(
    job_id: str,
    job_query: DocumentNode,
    complete_function: Callable[[dict], dict],
    timeout_seconds: Optional[int] = None,
    interval_seconds: Optional[int] = 30,
) -> dict:
    """
    Poll for status on a Dewrangle job

    If timeout is not set poll until job is complete.
    If timeout is set, poll until job is complete or timeout expires

    Wait interval_seconds between each status request to Dewrangle.

    Arguments:
        node_id - Dewrangle node ID of the job

        job_query - A GraphQL query to fetch the job

        complete_function - A method which determines when the job is
        complete and if it succeeded. This method will take in a dict containing
        the output of the graphql query and it must return a dict containing the
        following: { "complete": boolean, "success": boolean }

    Returns:
        a dict of the form {"success": boolean or None, "job": job_dict}

        success = True if job is complete without errors
        success = False if job is complete with errors
        success = None if timeout is set, and timeout is exceeded and
        job is not complete

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
        if status["complete"] or (not status["success"]):
            success = status["success"]
            emoji = "✅ " if success else "❌"
            suffix = "" if success else " with errors"
            logger.info(
                "%s Job %s %s completed%s:\n%s",
                emoji,
                operation,
                node_id,
                suffix,
                pformat(job),
            )

            return {"success": status["complete"], "job": job}

        elapsed_time_seconds = time.time() - start_time
        elapsed_formatted = time.strftime(
            "%H:%M:%S", time.gmtime(elapsed_time_seconds)
        )

        # Timeout exceeded
        if (timeout_seconds is not None) and (
            elapsed_time_seconds > timeout_seconds
        ):
            logger.warning(
                "⚠️  Timeout of %s seconds expired."
                " Current job %s %s result:\n%s"
                "\n✌️ Dewrangle must still be working, but CLI is exiting",
                timeout_seconds,
                operation,
                node_id,
                pformat(job),
            )
            return {"success": None, "job": job}

        # Continue polling
        logger.info(
            "⏰ Waiting for job %s %s to"
            " complete. Elapsed time (hh:mm:ss): %s",
            operation,
            node_id,
            elapsed_formatted,
        )

        time.sleep(interval_seconds)


def read_job(node_id: str, output_dir: str = DEWRANGLE_DIR) -> dict:
    """
    Fetch Job by ID from Dewrangle. Mostly for developer debugging purposes
    """
    params = {"id": node_id}

    resp = exec_query(queries.job, variables=params)
    result = resp["node"]
    logger.info("Fetched job %s", result["id"])
    operation = result["operation"].lower().replace("_", "-")

    errors = result["errors"]["edges"]
    if errors:
        logger.error("❌ Read job %s failed:\n%s", operation, pformat(errors))
    else:
        logger.info("🚦 Job-%s:\n%s", operation, pformat(result))

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"Job-{operation}.json")
        write_json(result, filepath)
        emoji = "❌" if errors else "✅"
        logger.info(
            "✏️  Wrote %s job to %s. %s  Found" " %s errors",
            operation,
            filepath,
            emoji,
            len(errors),
        )

    return result
