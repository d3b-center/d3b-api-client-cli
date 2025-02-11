"""
All CLI commands related to creating, updating, and downloading global IDs
in Dewrangle
"""

import logging
import click

from d3b_api_client_cli.config import log
from d3b_api_client_cli.dewrangle.global_id import (
    upsert_global_descriptors as _upsert_global_descriptors,
)

logger = logging.getLogger(__name__)


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
    Upsert global IDs in Dewrangle for a study.

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
