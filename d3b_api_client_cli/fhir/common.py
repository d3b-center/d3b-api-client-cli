"""
Common functions for FHIR package
"""

import os
import logging

from d3b_api_client_cli import utils
from d3b_api_client_cli.config import KidsFirstFhirEntity

logger = logging.getLogger(__name__)


def import_builders(entity_types=None):
    """
    Import FHIR entity builder classes from file
    """
    filters = None
    if entity_types:
        filters = set(entity_types)

    builders = {}
    for entity_type in KidsFirstFhirEntity:
        if filters and (entity_type.value not in filters):
            continue

        filepath = os.path.join(
            os.path.dirname(__file__),
            "entity_builders",
            f"{entity_type.value}",
            "__init__.py",
        )
        if not os.path.exists(filepath):
            logger.warning(
                f"⚠️  No entity builder exists for {entity_type.value}"
            )
            continue

        module = utils.import_module_from_file(filepath)
        cls_name = utils.snake_to_camel(entity_type.value)
        builders[entity_type.value] = getattr(module, cls_name)

    return builders
