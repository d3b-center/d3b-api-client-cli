"""
All CLI commands related to Dewrangle ingest operations

- Ingest study files into Dewrangle for a given study
- Upload single study file without starting an ingest job in Dewrangle
"""

import logging
import click

from d3b_api_client_cli.dewrangle.rest import (
    upload_study_file as _upload_study_file,
)
from d3b_api_client_cli.dewrangle import ingest

logger = logging.getLogger(__name__)


@click.command(help="Upload study file to Dewrangle study")
@click.argument(
    "dewrangle_study_id",
)
@click.argument(
    "filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
)
def upload_study_file(dewrangle_study_id, filepath):
    """
    Upload a clinical study file to Dewrangle

    \b
    Arguments:
      \b
      dewrangle_study_id - ID of the study in Dewrangle
      filepath - Path to the file to be uploaded
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info(
        f"🛸 Uploading study file: {filepath} to Dewrangle study:"
        f" {dewrangle_study_id}"
    )
    _upload_study_file(dewrangle_study_id, filepath)


@click.command(help="Ingest FHIR resource file into Dewrangle study")
@click.argument(
    "kf_study_id",
)
@click.argument(
    "filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
)
def ingest_study_file(kf_study_id, filepath):
    """
    Ingest a FHIR resource file into Dewrangle

    ** Only for debugging purposes **

    \b
    Arguments:
        \b
        kf_study_id - KF ID of the study in Dataservice
        filepath - Path to the file to be uploaded
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info(
        f"🏭 Ingesting study file: {filepath} to Dewrangle study:"
        f" {kf_study_id}"
    )
    ingest.upload_and_ingest_study_file(kf_study_id, filepath)


@click.command(
    help="Ingest a study's FHIR resource files into a Dewrangle study"
)
@click.argument(
    "study_data_dir_or_file",
    type=click.Path(exists=True, file_okay=True, dir_okay=True),
)
def ingest_study_files(study_data_dir_or_file):
    """
    Ingest a study's FHIR resource files into a Dewrangle study

    \b
    Arguments:
      \b
      study_data_dir - Path to the dir with FHIR json files to be ingested
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info(
        f"🏭 Ingesting study data in: {study_data_dir_or_file}" f" to Dewrangle"
    )

    return ingest.ingest_study_files(study_data_dir_or_file)
