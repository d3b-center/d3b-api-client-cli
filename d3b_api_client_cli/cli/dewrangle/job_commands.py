"""
Dewrangle Job commands
"""

import logging

import click

from d3b_api_client_cli.config.log import init_logger
from d3b_api_client_cli.dewrangle import graphql as gql_client
from d3b_api_client_cli.config import config

DEWRANGLE_DIR = config["dewrangle"]["output_dir"]

logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "node_id",
)
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where volumes will be written",
)
def read_job(node_id, output_dir):
    """
    Get Job in Dewrangle by Dewrangle GraphQL node ID

    \b
    Arguments:
      \b
      node_id - ID of the volume in Dewrangle
    """
    init_logger()

    return gql_client.read_job(node_id, output_dir)
