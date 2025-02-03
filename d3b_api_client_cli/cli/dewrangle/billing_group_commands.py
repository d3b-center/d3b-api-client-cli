"""
Dewrangle billing_group commands
"""

import logging

import click

from d3b_api_client_cli.config import config
from d3b_api_client_cli.config.log import init_logger
from d3b_api_client_cli.utils import read_json
from d3b_api_client_cli.dewrangle import graphql as gql_client

logger = logging.getLogger(__name__)
DEWRANGLE_DIR = config["dewrangle"]["output_dir"]


@click.command()
@click.option(
    "--cavatica-billing-group-id",
    required=True,
    help="Cavatica billing group ID",
)
@click.option(
    "--organization-id",
    required=True,
    help="ID of the Dewrangle org this billing_group will belong to",
)
def create_billing_group(cavatica_billing_group_id, organization_id):
    """
    Create billing_group in Dewrangle
    """
    init_logger()

    return gql_client.create_billing_group(
        organization_id, cavatica_billing_group_id
    )


@click.command()
@click.argument(
    "billing_group_id",
)
@click.option(
    "--disable-delete-safety-check",
    is_flag=True,
    help="This will allow deleting of the billing group",
)
def delete_billing_group(billing_group_id, disable_delete_safety_check):
    """
    Delete billing_group in Dewrangle by ID

    \b
    Arguments:
      \b
      billing_group_id - Dewrangle node ID
    """
    init_logger()

    return gql_client.delete_billing_group(
        billing_group_id, not disable_delete_safety_check
    )


@click.command()
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where billing_groups will be written",
)
def read_billing_groups(output_dir):
    """
    Fetch billing_groups from Dewrangle
    """
    init_logger()

    return gql_client.read_billing_groups(output_dir)


@click.command()
@click.argument(
    "node_id",
)
def get_billing_group(node_id):
    """
    Get billing_group in Dewrangle by Dewrangle GraphQL node ID

    \b
    Arguments:
      \b
      node_id - ID of the billing_group in Dewrangle
    """
    init_logger()

    return gql_client.read_billing_group(node_id)
