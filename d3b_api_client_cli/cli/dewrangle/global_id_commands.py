"""
All CLI commands related to creating, updating, and downloading global IDs
in Dewrangle
"""

import logging
import click

from d3b_api_client_cli.dewrangle.rest import (
    request_global_ids as _upsert_global_ids,
    download_global_ids as _download_global_ids,
)

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--dewrangle-job-id",
    help="The job id returned from upserting global IDs. If provided, "
    "only global IDs from the recent upsert request will be downloaded",
)
@click.argument(
    "kf_study_id",
)
@click.argument(
    "filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
)
def download_global_ids(kf_study_id, filepath, dewrangle_job_id):
    """
    Download global IDs for a study in Dewrangle

    \b
    Arguments:
      \b
      kf_study_id - Kids First ID of the study in Dataservice
      filepath - Path the file that you want global IDs to be written to
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info(
        f"🛸 Downloading global ID file to {filepath} from Dewrangle study:"
        f" {kf_study_id}"
    )
    what = "all"
    if dewrangle_job_id:
        what = f"job {dewrangle_job_id}"

    logger.info(
        f"🛸 Downloading {what} global IDs from Dewrangle to file: "
        f"{filepath} for study {kf_study_id}"
    )
    _download_global_ids(kf_study_id, filepath, dewrangle_job_id)


@click.command()
@click.argument(
    "kf_study_id",
)
@click.argument(
    "filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
)
def upsert_global_ids(kf_study_id, filepath):
    """
    Request global IDs to be created or updated in Dewrangle for a study.

    In order to create new global IDs provide a CSV file with the columns:
    descriptor, fhirResourceType

    In order to update existing global IDs provide a CSV file with the columns:
    descriptor, fhirResourceType, globalId

    \b
    Arguments:
      \b
      kf_study_id - Kids First ID of the study in Dataservice
      filepath - Path to the global ID request file
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info(
        "🛸 Requesting global IDs from Dewrangle for descriptors in "
        f"{filepath} and study {kf_study_id}"
    )
    _upsert_global_ids(kf_study_id, filepath)
