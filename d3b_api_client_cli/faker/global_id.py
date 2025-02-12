"""
Generate files of global ID descriptors for testing and development
"""

import os
from typing import Optional
from pprint import pformat
import logging

import pandas

from d3b_api_client_cli.config import (
    config,
    FhirResourceType,
    ROOT_FAKE_DATA_DIR,
)

FHIR_RESOURCE_TYPES: dict[str, FhirResourceType] = config["faker"]["global_id"][
    "fhir_resource_types"
]
DEFAULT_FHIR_RESOURCE_TYPE: str = "DocumentReference"

logger = logging.getLogger(__name__)


def _generate_fake_global_id(
    prefix: str, starting_index: Optional[int] = 0
) -> str:
    """
    Generate a fake Dewrangle global ID
    """
    starting_index = str(starting_index)

    if not starting_index.isdigit():
        raise ValueError("Starting index must contain only digits.")

    if len(starting_index) > 10:
        raise ValueError("Starting index cannot be longer than 10 digits.")

    remaining_length = 10 - len(starting_index)
    remaining = "0" * remaining_length

    return f"{prefix}-{str(starting_index)}{remaining}"


def generate_global_id_file(
    fhir_resource_type: Optional[str] = DEFAULT_FHIR_RESOURCE_TYPE,
    with_global_ids: Optional[bool] = False,
    total_rows: Optional[int] = 10,
    starting_index: Optional[int] = 0,
    output_dir: Optional[str] = None,
) -> str:
    """
    Generate a csv file with global IDs and descriptors.

    Descriptors are formatted like:

        - <2 char prefix for resource type>-<row index>000
        - Example: dr-1000

    When starting_index is supplied it will be added to the row index.

        - Example: row 0, starting_index=255, descriptor = dr-25500
        - Example: row 1, starting_index=255, descriptor = dr-25600

    The starting_index allows a developer to have some control over the
    descriptors that get generated so they can test create, replace, and
    append functions for descriptors.

    Options:
        - fhir_resource_type: the FHIR resource type and global ID prefix
        to populate the file with

        - with_global_ids: Whether or not to include a column for global IDs
        if global IDs are not included and this file is used in
        upsert_global_descriptors, then new global IDs will be created by
        Dewrangle

        - total_rows: Number of rows to generate

        - starting_index: Used in generating sequential descriptors.

    Returns:
        Path to file
    """
    logger.info(
        "🏭 Generating %s rows for fake global ID descriptors file", total_rows
    )
    if not output_dir:
        output_dir = ROOT_FAKE_DATA_DIR
        os.makedirs(output_dir, exist_ok=True)

    fhir_resource_type = FHIR_RESOURCE_TYPES.get(fhir_resource_type)

    data = []
    for i in range(total_rows):
        rt = fhir_resource_type.resource_type
        global_id = _generate_fake_global_id(
            fhir_resource_type.id_prefix, starting_index + i
        )
        descriptor_suffix = global_id.split("-")[-1]
        row = {
            "fhirResourceType": fhir_resource_type.resource_type,
            "descriptor": f"{rt}-{descriptor_suffix}",
        }
        if with_global_ids:
            row["globalId"] = global_id
        data.append(row)

        logger.info("Wrote %s to file", pformat(row))

    df = pandas.DataFrame(data)

    filepath = os.path.join(output_dir, "fake_global_descriptors.csv")
    df.to_csv(filepath, index=False)

    logger.info("✅ Completed writing global ID descriptors to %s", filepath)

    return filepath
