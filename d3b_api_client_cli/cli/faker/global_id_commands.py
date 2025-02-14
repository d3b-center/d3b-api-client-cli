"""
Commands to generate fake global ID descriptors
"""

import os
import logging
import click

from d3b_api_client_cli.config import log, FHIR_RESOURCE_TYPES, FhirResourceType
from d3b_api_client_cli.faker.global_id import (
    generate_global_id_file as _generate_global_id_file,
)

logger = logging.getLogger(__name__)

DEFAULT_FHIR_RESOURCE_TYPE: FhirResourceType = FHIR_RESOURCE_TYPES[
    "DocumentReference"
]


@click.command()
@click.option(
    "--output-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Where the output file will be written",
)
@click.option(
    "--fhir-resource-type",
    default=DEFAULT_FHIR_RESOURCE_TYPE.resource_type,
    type=click.Choice([rt for rt in FHIR_RESOURCE_TYPES.keys()]),
    help="What the fhirResourceType column will be populated with",
)
@click.option(
    "--with-global-ids",
    is_flag=True,
    help="Whether or not to generate a globalId column",
)
@click.option(
    "--starting-index",
    type=int,
    default=0,
    help="Determines what index the sequential descriptors start at",
)
@click.option(
    "--total-rows",
    type=int,
    default=10,
    help="Total number of rows to generate",
)
def generate_global_id_file(
    total_rows, starting_index, with_global_ids, fhir_resource_type, output_dir
):
    """
    Generate a csv file with global IDs and descriptors.

    \b
    Descriptors are formatted like:
        \b
        - <2 char prefix for resource type>-<row index>000
        - Example: For a DocumentReference FHIR resource type the
        descriptors would look like `dr-1000`

    \b
    When starting_index is supplied it will be added to the row index.
        \b
        - Example: row 0, starting_index=255, descriptor = dr-25500
        - Example: row 1, starting_index=255, descriptor = dr-25600

    \b
    The starting_index allows a developer to have some control over the
    descriptors that get generated so they can test create, replace, and
    append functions for global IDs.
    """

    log.init_logger()

    return _generate_global_id_file(
        fhir_resource_type,
        total_rows=total_rows,
        starting_index=starting_index,
        with_global_ids=with_global_ids,
        output_dir=output_dir,
    )
