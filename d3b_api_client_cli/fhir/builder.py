"""
Iterate over FHIR entity builders, import from module and run the build to
produce FHIR JSON
"""

import os
import logging
import shutil
from pprint import pformat

from d3b_api_client_cli import utils
from d3b_api_client_cli.fhir.common import import_builders
from d3b_api_client_cli.config import (
    FHIR_JSON_DIR,
    KidsFirstFhirEntity,
)

logger = logging.getLogger(__name__)
valid_kids_first_fhir_types = set([et.value for et in KidsFirstFhirEntity])


def build_entities(
    source_dir, dest_dir=None, kf_fhir_entity_types=None, cleanup=False
):
    """
    Build Kids First FHIR resources

    If kf_fhir_entity_types=None, produce all entities. Otherwise, only
    produce the entities specified by kf_fhir_entity_types

    :param source_dir: dir where tables will be read from
    :type source_dir: str
    :param dest_dir: dir where merged data will be written
    :type dest_dir: str
    :param kf_fhir_entity_types: if not None, only build these entity types
    :type kf_fhir_entity_types: str
    :param cleanup: if true, delete the contents of ETL stage directories
    before running ETL
    :type cleanup: boolean

    :rtype: list of dicts
    :returns: FHIR resource dicts
    """
    if not kf_fhir_entity_types:
        entities_to_build = valid_kids_first_fhir_types
    else:
        entities_to_build = set(kf_fhir_entity_types)

    # Get study id
    fp = os.path.join(source_dir, "Study.json")
    study = utils.read_json(fp)
    study_id = study["kf_id"]
    parent_study_id = study["parent_study_id"]

    if cleanup:
        logger.info(f"🚮 Removing old fhir directory: {dest_dir}")
        shutil.rmtree(dest_dir, ignore_errors=True)

    # Setup output dir
    if not dest_dir:
        dest_dir = os.path.join(FHIR_JSON_DIR, study_id)
    os.makedirs(dest_dir, exist_ok=True)

    # Copy study definition file to output
    src = fp
    dst = os.path.join(dest_dir, "Study.json")
    shutil.copyfile(src, dst)

    builders = import_builders()
    logger.info(f"Found builders: {pformat(builders.keys())}")

    counts = {}
    # For each fhir entity, import the entity builder
    for entity_type, builder_class in builders.items():
        if entity_type not in entities_to_build:
            logger.info(
                f"⏭️  Skip building {entity_type}. User did not include it in"
                f" input options: {pformat(kf_fhir_entity_types)}"
            )
            continue

        # Build the target entities for FHIR
        builder = builder_class(study_id, parent_study_id=parent_study_id)
        try:
            resources = builder.build(source_dir, dest_dir)
        except FileNotFoundError as e:
            logger.error(
                f"‼️  Error building {entity_type}. Missing data files:"
                f" {str(e)}"
            )
            continue

        counts[entity_type] = len(resources)

        if len(resources):
            # Write entities to file
            out_fp = os.path.join(dest_dir, f"{entity_type}.json")
            utils.write_json(resources, out_fp)
            parts = os.path.split(out_fp)

            logger.info(f"✏️  Wrote {parts[-1]} to {parts[0]}")

    logger.info(
        f"✅ Completed building FHIR JSON for {study_id}:\n{pformat(counts)}"
    )
