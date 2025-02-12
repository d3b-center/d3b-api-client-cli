"""
All CLI commands related to creating, updating, and downloading global IDs
in Dewrangle
"""

import os
import logging
import click

from d3b_api_client_cli.config import log
from d3b_api_client_cli.dewrangle.global_id import GlobalIdDescriptorOptions
from d3b_api_client_cli.dewrangle.global_id import (
    upsert_global_descriptors as _upsert_global_descriptors,
    download_global_descriptors as _download_global_descriptors,
    upsert_and_download_global_descriptors as _upsert_and_download_global_descriptors,
)

logger = logging.getLogger(__name__)



@click.command()
@click.option(
    "--output-filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
    help="If provided, download the file to this path. This takes "
    "precedence over the --output-dir option"
)
@click.option(
    "--output-dir",
    default=os.getcwd(),
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="If provided, download the file with the default file name into "
    "this directory"
)
@click.option(
    "--download-all",
    is_flag=True,
    help="What descriptor(s) for each global ID to download. Either download"
    " all descriptors for each global ID or just the most recent"
)
@click.option(
    "--study-global-id",
    help="The global ID of the study in Dewrangle. You must provide either "
    "the global ID of the study OR the GraphQL ID of the study but not both"
)
@click.option(
    "--study-id",
    help="The GraphQL ID of the study in Dewrangle. You must provide either "
    "the global ID of the study OR the GraphQL ID of the study but not both"
)
@click.argument(
    "input_filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
)
def upsert_and_download_global_descriptors(
    input_filepath, study_id, study_global_id, download_all, output_dir,
    output_filepath
):
    """
    Send request to upsert global ID descriptors in Dewrangle and 
    download the resulting global ID descriptors.

    In order to create new global IDs provide a CSV file with the columns:
    descriptor, fhirResourceType

    In order to update existing global IDs provide a CSV file with the columns:
    descriptor, fhirResourceType, globalId

    \b
    Arguments:
      \b
      input_filepath - Path to the file with global IDs and descriptors
    """

    log.init_logger()

    if (not study_id) and (not study_global_id):
        raise click.BadParameter(
            "❌ You must provide either the study's global ID in Dewrangle OR "
            "the study's GraphQL ID in Dewrangle"
        )

    return _upsert_and_download_global_descriptors(
        input_filepath,
        study_global_id=study_global_id,
        dewrangle_study_id=study_id,
        download_all=download_all,
        output_dir=output_dir,
        output_filepath=output_filepath,
    )


@click.command()
@click.option(
    "--study-global-id",
    help="The global ID of the study in Dewrangle. You must provide either "
    "the global ID of the study OR the GraphQL ID of the study but not both"
)
@click.option(
    "--study-id",
    help="The GraphQL ID of the study in Dewrangle. You must provide either "
    "the global ID of the study OR the GraphQL ID of the study but not both"
)
@click.argument(
    "filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
)
def upsert_global_descriptors(filepath, study_id, study_global_id):
    """
    Upsert global ID descriptors in Dewrangle for a study.

    In order to create new global IDs provide a CSV file with the columns:
    descriptor, fhirResourceType

    In order to update existing global IDs provide a CSV file with the columns:
    descriptor, fhirResourceType, globalId

    \b
    Arguments:
      \b
      filepath - Path to the file with global IDs and descriptors
    """

    log.init_logger()

    if (not study_id) and (not study_global_id):
        raise click.BadParameter(
            "❌ You must provide either the study's global ID in Dewrangle OR "
            "the study's GraphQL ID in Dewrangle"
        )

    return _upsert_global_descriptors(filepath, study_global_id, study_id)


@click.command()
@click.option(
    "--output-dir",
    default=os.getcwd(),
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="If provided, download the file with the default file name into "
    "this directory"
)
@click.option(
    "--download-all",
    is_flag=True,
    help="What descriptor(s) for each global ID to download. Either download"
    " all descriptors for each global ID or just the most recent"
)
@click.option(
    "--job-id",
    help="Dewrangle job id from the upsert_global_descriptors cmd"
)
@click.option(
    "--study-global-id",
    help="The global ID of the study in Dewrangle. You must provide either "
    "the global ID of the study OR the GraphQL ID of the study but not both"
)
@click.option(
    "--study-id",
    help="The GraphQL ID of the study in Dewrangle. You must provide either "
    "the global ID of the study OR the GraphQL ID of the study but not both"
)
@click.option(
    "--filepath",
    type=click.Path(exists=False, file_okay=True, dir_okay=False),
    help="If provided, download the file to this filepath. This takes "
    "precedence over --output-dir"
)
def download_global_descriptors(
    filepath, study_id, study_global_id, job_id, download_all, output_dir
):
    """
    Download global ID descriptors in Dewrangle for a study.
    """

    log.init_logger()

    if (not study_id) and (not study_global_id):
        raise click.BadParameter(
            "❌ You must provide either the study's global ID in Dewrangle OR "
            "the study's GraphQL ID in Dewrangle"
        )

    return _download_global_descriptors(
        dewrangle_study_id=study_id,
        study_global_id=study_global_id,
        filepath=filepath,
        job_id=job_id,
        download_all=download_all,
        output_dir=output_dir,
    )
