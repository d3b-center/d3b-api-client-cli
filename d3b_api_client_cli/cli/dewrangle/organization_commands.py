"""
Dewrangle CLI commands

Functions for interacting the Dewrangle's GraphQL and REST APIs
"""

import logging

import click

from d3b_api_client_cli.config import config
from d3b_api_client_cli.config.log import init_logger
from d3b_api_client_cli.utils import read_json
from d3b_api_client_cli.dewrangle import graphql as gql_client

from pprint import pprint

logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
def upsert_organization(filepath):
    """
    Upsert organization in Dewrangle. Used in integration testing

    \b
    Arguments:
      \b
      filepath - Path to file defining Dewrangle organization
    """
    init_logger()

    return gql_client.upsert_organization(read_json(filepath))


@click.command()
@click.option(
    "--disable-delete-safety-check",
    is_flag=True,
    help="This will allow deleting of the organization",
)
@click.option(
    "--dewrangle-org-id",
    help="The Dewrangle GraphQL node ID of organization",
)
@click.option(
    "--dewrangle-org-name",
    help="The Dewrangle name of organization",
)
def delete_organization(
    dewrangle_org_id, dewrangle_org_name, disable_delete_safety_check
):
    """
    Delete organization in Dewrangle by either ID or name. Used in integration
    testing
    """
    init_logger()

    if dewrangle_org_id:
        kwargs = {"dewrangle_org_id": dewrangle_org_id}
    else:
        kwargs = {"dewrangle_org_name": dewrangle_org_name}

    kwargs["delete_safety_check"] = not disable_delete_safety_check

    pprint(kwargs)

    return gql_client.delete_organization(**kwargs)


@click.command()
@click.option(
    "--output-dir",
    default=config["dewrangle"]["output_dir"],
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where organizations will be written",
)
def read_organizations(output_dir):
    """
    Fetch organizations from Dewrangle. Used in integration testing
    """
    init_logger()

    return gql_client.read_organizations(output_dir)
