"""
Commands related to initializing Dewrangle with data
"""

import logging
import click


from d3b_api_client_cli.config import (
    DEWRANGLE_FHIR_SERVERS_FILEPATH,
)
from d3b_api_client_cli.utils import read_json
from d3b_api_client_cli.dewrangle import setup

logger = logging.getLogger(__name__)


@click.command(
    help="Upsert Kids First study in Dewrangle. Attach FHIR servers to study"
)
@click.option(
    "--dewrangle-fhir-servers-file",
    default=DEWRANGLE_FHIR_SERVERS_FILEPATH,
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path to file containing Dewrangle FHIR server configuration",
)
@click.argument("kf_study_id")
@click.argument("dewrangle_org_name")
def setup_dewrangle_study(
    kf_study_id, dewrangle_org_name, dewrangle_fhir_servers_file
):
    """
    Upsert study in Dewrangle. Attach FHIR servers to study

    \b
    Arguments:
      \b
      kf_study_id - ID of the study in Dataservice
      dewrangle_org_name - Name of the Dewrangle organization to add study to
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info("✨ Setting up study in Dewrangle")
    fhir_servers = read_json(dewrangle_fhir_servers_file)

    return setup.setup_dewrangle_study(
        kf_study_id,
        fhir_servers,
        dewrangle_org_name=dewrangle_org_name,
    )


@click.command(
    help="Upsert organization in Dewrangle. Upsert all studies in "
    "Dataservice into org. Attach FHIR servers to each study"
)
@click.option(
    "--organization-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path to file containing the Dewrangle organization payload",
)
@click.option(
    "--with-studies",
    is_flag=True,
    help="Fetch all studies from Dataservice, upsert in Dewrangle, and "
    "attach the FHIR servers to those studies",
)
@click.option(
    "--dewrangle-fhir-servers-file",
    default=DEWRANGLE_FHIR_SERVERS_FILEPATH,
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path to file containing Dewrangle FHIR server configuration",
)
def setup_dewrangle_org(
    with_studies, dewrangle_fhir_servers_file, organization_file
):
    """
    Upsert organization in Dewrangle. Upsert all studies in
    Dataservice into org. Attach FHIR servers to each study
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info("✨ Setting up organization in Dewrangle")

    org = None
    if organization_file:
        org = read_json(organization_file)

    return setup.setup_dewrangle_org(
        organization_payload=org,
        with_studies=with_studies,
        fhir_servers_filepath=dewrangle_fhir_servers_file,
    )


@click.command(
    help="Setup all Dataservice studies within Dewrangle organization."
    " Upsert all studies in Dataservice into org then attach the org's"
    " FHIR servers to each study"
)
@click.option(
    "--dataservice-study-ids-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path to JSON file containing a list of study KF IDs",
)
@click.option(
    "--dewrangle-fhir-servers-file",
    default=DEWRANGLE_FHIR_SERVERS_FILEPATH,
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    help="The path to file containing Dewrangle FHIR server configuration",
)
@click.argument("dewrangle_org_name")
def setup_all_studies(
    dewrangle_org_name, dewrangle_fhir_servers_file, dataservice_study_ids_file
):
    """
    Setup all Dataservice studies within Dewrangle organization.
    Upsert all studies in Dataservice into org then attach the org's
    FHIR servers to each study
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    logger.info(
        "✨ Setting up all Dataservice studies in Dewrangle organization:"
        f" {dewrangle_org_name}"
    )

    study_ids = None
    if dataservice_study_ids_file:
        study_ids = read_json(dataservice_study_ids_file)

    fhir_servers = read_json(dewrangle_fhir_servers_file)

    return setup.setup_all_studies(
        fhir_servers, study_ids=study_ids, dewrangle_org_name=dewrangle_org_name
    )
