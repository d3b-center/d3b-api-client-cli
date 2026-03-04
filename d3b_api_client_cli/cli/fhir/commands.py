"""
All CLI commands related to FHIR operations
"""

import logging
from pprint import pformat
import click

from d3b_api_client_cli.config import (
    config,
    valid_kids_first_fhir_types,
    valid_fhir_types,
)
from d3b_api_client_cli import utils
from d3b_api_client_cli.fhir import builder, loader, counts
from d3b_api_client_cli.fhir import delete as trasher

logger = logging.getLogger(__name__)


@click.command(help="Fetch total counts of KF FHIR types in FHIR server")
@click.argument(
    "kf_study_id",
)
def total_counts(kf_study_id):
    """
    Fetch total counts of KF FHIR types in FHIR server. Types are listed in
    d3b_api_client_cli.config.KidsFirstFhirEntity

    \b
    Arguments:
      \b
      kf_study_id - Kids First ID of study in Dataservice
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return counts.get_counts(kf_study_id)


@click.command(help="Transform Dataservice tables into FHIR resource tables")
@click.option(
    "--entity-types",
    help="Comma delimited list of the Kids First FHIR entity types to build."
    f" Must be one or more of: {pformat(valid_kids_first_fhir_types)}",
)
@click.option(
    "--dest-dir",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The dir path to where FHIR JSON will be written by" " build process",
)
@click.argument(
    "source_dir",
)
def build(source_dir, dest_dir, entity_types):
    """
    Transform Dataservice tables into FHIR JSON

    \b
    Arguments:
      \b
      source_dir - Directory of tables to merge
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    if entity_types:
        entity_types = [
            et.strip() for et in utils.multisplit(entity_types, [","])
        ]
        if not set(entity_types) <= valid_kids_first_fhir_types:
            raise click.BadParameter(
                "Bad --entity-types value. Must be comma delimited list "
                "where each item is one of: "
                f"{pformat(valid_kids_first_fhir_types)}"
            )
    suffix = "all" if not entity_types else entity_types
    logger.info(
        f"🏭 Building FHIR JSON from Dataservice tables for {suffix} entities"
    )
    builder.build_entities(
        source_dir,
        dest_dir=dest_dir,
        kf_fhir_entity_types=entity_types,
    )


@click.command(name="load", help="Load FHIR JSON data into FHIR service")
@click.option(
    "--fhir-url",
    default=config["fhir"]["base_url"],
    help="The base url of the FHIR service",
)
@click.option(
    "--ignore-load-errors",
    default=False,
    help="Whether to let the FHIR loader keep going even if it encounters "
    "load errors",
)
@click.argument(
    "source_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
def load_fhir(source_dir, fhir_url, ignore_load_errors):
    """
    Load data into FHIR service

    \b
    Arguments:
      \b
      source_dir - Directory of FHIR JSON files
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info("🏭 Start loading data into FHIR service ...")
    loader.load_data(
        fhir_url, data_dir=source_dir, ignore_load_errors=ignore_load_errors
    )


@click.command(help="Delete FHIR resources in FHIR service")
@click.option(
    "--disable-safety-check",
    is_flag=True,
    help="The base url of the FHIR service",
)
@click.option(
    "--entity-types",
    help="Comma delimited list of the Kids First FHIR entity types to delete."
    f" Must be one or more of: {pformat(valid_kids_first_fhir_types)}",
)
@click.option(
    "--fhir-url",
    default=config["fhir"]["base_url"],
    help="The base url of the FHIR service",
)
@click.argument(
    "source_dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
def delete_from_file(source_dir, fhir_url, entity_types, disable_safety_check):
    """
    Delete the given FHIR resources in the FHIR service

    \b
    Arguments:
      \b
      source_dir - Directory FHIR JSON files
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    if entity_types:
        entity_types = [
            et.strip() for et in utils.multisplit(entity_types, [","])
        ]
        if not set(entity_types) <= valid_kids_first_fhir_types:
            raise click.BadParameter(
                "Bad --entity-types value. Must be comma delimited list "
                "where each item is one of: "
                f"{pformat(valid_kids_first_fhir_types)}"
            )

    safety_check = not disable_safety_check
    logger.info(
        f"🚮 Start deleting data in FHIR service. SAFETY_CHECK: {safety_check}"
    )
    if safety_check:
        logger.info(
            "When safety check is enabled, only resources in localhost"
            " can be deleted"
        )
    trasher.delete_from_file(
        fhir_url,
        source_dir,
        entity_types=entity_types,
        safety_check=safety_check,
    )


def _delete_all(
    study_id,
    fhir_url,
    entity_types,
    output_dir,
    disable_safety_check,
    legacy_server,
):
    """
    Helper for delete-all and delete-study cmds
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    if entity_types:
        entity_types = [
            et.strip() for et in utils.multisplit(entity_types, [","])
        ]
        if not set(entity_types) <= valid_fhir_types:
            raise click.BadParameter(
                "Bad --entity-types value. Must be comma delimited list "
                "where each item is one of: "
                f"{pformat(valid_fhir_types)}"
            )
    else:
        entity_types = valid_fhir_types

    safety_check = not disable_safety_check
    logger.info(
        f"🚮 Start deleting data in FHIR service: {fhir_url} "
        f"SAFETY_CHECK: {safety_check}"
    )
    if safety_check:
        logger.info(
            "When safety check is enabled, only resources in localhost"
            " can be deleted"
        )

    if legacy_server:
        use_kf_entity_tags = False
    else:
        use_kf_entity_tags = True

    trasher.delete_all(
        fhir_url,
        entity_types=entity_types,
        output_dir=output_dir,
        study_id=study_id,
        safety_check=safety_check,
        use_kf_entity_tags=use_kf_entity_tags,
    )


@click.command(help="Delete all FHIR resources in FHIR service by entity_type")
@click.option(
    "--legacy-server",
    is_flag=True,
    help="If the FHIR service is a legacy server (without OAuth2)",
)
@click.option(
    "--disable-safety-check",
    is_flag=True,
    help="The base url of the FHIR service",
)
@click.option(
    "--output-dir",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to dir where delete results will be written",
)
@click.option(
    "--study-id",
    help="The KF ID of the study to delete data for",
)
@click.option(
    "--entity-types",
    help="Comma delimited list of the FHIR resource types to delete."
    f" Must be one or more of: {pformat(valid_kids_first_fhir_types)}",
)
@click.option(
    "--fhir-url",
    default=config["fhir"]["base_url"],
    help="The base url of the FHIR service",
)
def delete_all(
    fhir_url,
    entity_types,
    study_id,
    output_dir,
    disable_safety_check,
    legacy_server,
):
    """
    Delete FHIR resources by entity_type in the FHIR service
    """
    _delete_all(
        study_id,
        fhir_url,
        entity_types,
        output_dir,
        disable_safety_check,
        legacy_server,
    )


@click.command(
    help="Delete all FHIR resources in FHIR service by study."
    " Different from delete-all since it requires a study"
)
@click.option(
    "--legacy-server",
    is_flag=True,
    help="If the FHIR service is a legacy server (without OAuth2)",
)
@click.option(
    "--disable-safety-check",
    is_flag=True,
    help="The base url of the FHIR service",
)
@click.option(
    "--output-dir",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to dir where delete results will be written",
)
@click.option(
    "--entity-types",
    help="Comma delimited list of the FHIR resource types to delete."
    f" Must be one or more of: {pformat(valid_kids_first_fhir_types)}",
)
@click.option(
    "--fhir-url",
    default=config["fhir"]["base_url"],
    help="The base url of the FHIR service",
)
@click.argument(
    "kf_study_id",
)
def delete_fhir_study(
    kf_study_id,
    fhir_url,
    entity_types,
    output_dir,
    disable_safety_check,
    legacy_server,
):
    """
    Delete FHIR resources by entity_type in the FHIR service
    """

    _delete_all(
        kf_study_id,
        fhir_url,
        entity_types,
        output_dir,
        disable_safety_check,
        legacy_server,
    )
