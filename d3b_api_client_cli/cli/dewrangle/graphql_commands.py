"""
All CLI commands related to basic Dewrangle graphql operations

- CRUD Dewrangle organization
- CRUD Dewrangle study
"""

import logging

import click

from d3b_api_client_cli.config import (
    IdTypes,
    DEWRANGLE_DIR,
)
from d3b_api_client_cli.utils import read_json
from d3b_api_client_cli.dewrangle import graphql as gql_client

logger = logging.getLogger(__name__)


@click.command(help="Delete FHIR server in Dewrangle")
@click.argument(
    "dewrangle_organization_id",
)
def delete_fhir_server(dewrangle_organization_id):
    """
    Delete FHIR server in Dewrangle

    \b
    Arguments:
      \b
      dewrangle_organization_id - Dewrangle ID of organization
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.delete_fhir_server(dewrangle_organization_id)


@click.command(help="Upsert FHIR server in Dewrangle")
@click.option(
    "--oidc-client-secret",
    help="The secret for the Keycloak OIDC client that will be used to "
    " authenticate with the FHIR server before releasing data to it",
)
@click.argument(
    "filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.argument(
    "dewrangle_organization_id",
)
def upsert_fhir_server(dewrangle_organization_id, filepath, oidc_client_secret):
    """
    Upsert FHIR server in Dewrangle

    \b
    Arguments:
      \b
      dewrangle_organization_id - Dewrangle ID of organization
      filepath - Path to file defining Dewrangle organization
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.upsert_fhir_server(
        dewrangle_organization_id,
        read_json(filepath),
        oidc_client_secret=oidc_client_secret,
    )


@click.command(help="Fetch FHIR servers from Dewrangle")
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where fhir servers will be written",
)
@click.argument(
    "dewrangle_organization_id",
)
def read_fhir_servers(dewrangle_organization_id, output_dir):
    """
    Fetch FHIR servers from Dewrangle

    \b
    Arguments:
      \b
      dewrangle_organization_id - Dewrangle ID of organization
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.read_fhir_servers(dewrangle_organization_id, output_dir)


@click.command(help="Fetch FHIR ingest job from Dewrangle")
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where organizations will be written",
)
@click.argument(
    "node_id",
)
def read_fhir_ingest_job(node_id, output_dir):
    """
    Fetch FHIR ingest job from Dewrangle

    \b
    Arguments:
      \b
      node_id - Dewrangle ID of FHIR ingest job
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.read_fhir_ingest_job(node_id, output_dir)


@click.command(help="Upsert organization in Dewrangle")
@click.argument(
    "filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
def upsert_organization(filepath):
    """
    Upsert organization in Dewrangle

    \b
    Arguments:
      \b
      filepath - Path to file defining Dewrangle organization
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.upsert_organization(read_json(filepath))


@click.command(help="Delete organization in Dewrangle by either ID or name")
@click.option(
    "--dewrangle-org-id",
    help=f"The Dewrangle GraphQL node ID of organization ",
)
@click.option(
    "--dewrangle-org-name",
    help=f"The Dewrangle name of organization ",
)
def delete_organization(dewrangle_org_id, dewrangle_org_name):
    """
    Delete organization in Dewrangle by either ID or name

    \b
    Arguments:
      \b
      node_id - ID of the organization in Dewrangle
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    if dewrangle_org_id:
        kwargs = {"dewrangle_org_id": dewrangle_org_id}
    else:
        kwargs = {"dewrangle_org_name": dewrangle_org_name}
    return gql_client.delete_organization(**kwargs)


@click.command(help="Fetch organizations from Dewrangle")
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where organizations will be written",
)
def read_organizations(output_dir):
    """
    Fetch organizations from Dewrangle
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.read_organizations(output_dir)


@click.command(help="Upsert study in Dewrangle")
@click.option(
    "--kf-study-id",
    help="The KF ID of the study to use as the Dewrangle global ID",
)
@click.argument(
    "filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.argument(
    "organization_id",
)
def upsert_study(filepath, organization_id, kf_study_id):
    """
    Upsert study in Dewrangle

    \b
    Arguments:
      \b
      filepath - Path to file defining Dewrangle study
      organization_id - ID of the Dewrangle org this study will belong to
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    if not kf_study_id:
        data = read_json(filepath)
        kf_id = data.pop("kf_id", None)
    else:
        kf_id = kf_study_id

    return gql_client.upsert_study(data, organization_id, kf_study_id=kf_id)


@click.command(help="Delete study in Dewrangle")
@click.option(
    "--id-type",
    default=IdTypes.DEWRANGLE.value,
    help=f"The type of ID. Must be one of: {[i.value for i in IdTypes]} ",
)
@click.argument(
    "node_id",
)
def delete_study(node_id, id_type):
    """
    Delete study in Dewrangle

    \b
    Arguments:
      \b
      node_id - ID of the study in Dewrangle
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.delete_study(node_id, id_type)


@click.command(help="Fetch studies from Dewrangle")
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where studies will be written",
)
def read_studies(output_dir):
    """
    Fetch studies from Dewrangle
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.read_studies(output_dir)


@click.command(help="Get study in Dewrangle by Dewrangle node ID")
@click.argument(
    "node_id",
)
def get_study(node_id):
    """
    Get study in Dewrangle by Dewrangle GraphQL node ID

    \b
    Arguments:
      \b
      node_id - ID of the study in Dewrangle
    """
    from d3b_api_client_cli.config import log

    log.init_logger()

    return gql_client.read_study(node_id)
