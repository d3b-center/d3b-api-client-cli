"""
Common functions for CLI commands. Mostly parameter validators
"""

from pprint import pformat
from typing import List

from urllib.parse import urlparse
import click

from d3b_api_client_cli import utils
from d3b_api_client_cli.config import (
    KidsFirstFhirEntity,
    ETL_STAGES,
    config,
)


def validate_kids_first_types(types: List[str]):
    """
    Validate Kids First Dataservice types
    """
    endpoints = config["dataservice"]["endpoints"]

    kids_first_types = set(types)
    default = set(utils.camel_to_snake(e) for e in endpoints)

    if not kids_first_types <= default:
        invalid = kids_first_types - default
        raise click.BadParameter(
            f"Invalid Kids First Dataservice Type: {pformat(invalid)}."
            f" Each type must be one of {pformat(default)}"
        )


def validate_url(ctx, param, url: str):
    """
    Ensure url is valid
    """
    try:
        result = urlparse(url)
        if not (result.scheme and result.netloc):
            raise click.BadParameter(f"{url} is not a valid URL")
        return url
    except Exception as exc:
        raise click.BadParameter(f"{url} is not a valid URL") from exc


def validate_kids_first_fhir_types(types: List[str]):
    """
    Validate kids_first_fhir_types
    """
    kids_first_fhir_types = set(types)
    default = {v.value for v in KidsFirstFhirEntity}

    if not kids_first_fhir_types <= default:
        invalid = kids_first_fhir_types - default
        raise click.BadParameter(
            f"Invalid Kids First FHIR Type: {pformat(invalid)}."
            f" Each type must be one of {pformat(default)}"
        )


def validate_stages(stages):
    """
    Validate stages CLI option
    """
    if not all(s in set(ETL_STAGES) for s in stages):
        raise click.BadParameter(
            f"Invalid stages value {stages}. Must one or more chars in:"
            f" '{ETL_STAGES}'"
        )
