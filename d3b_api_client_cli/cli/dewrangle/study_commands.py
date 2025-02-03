"""
Dewrangle study commands
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
    "--study-id",
    help="Either the KF ID or Dewrangle global ID of the study",
)
@click.argument(
    "filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.argument(
    "organization_id",
)
def upsert_study(filepath, organization_id, study_id):
    """
    Upsert study in Dewrangle

    \b
    Arguments:
      \b
      filepath - Path to file defining Dewrangle study
      organization_id - ID of the Dewrangle org this study will belong to
    """
    init_logger()

    if not study_id:
        data = read_json(filepath)
        kf_id = data.pop("kf_id", None)
    else:
        kf_id = study_id

    return gql_client.upsert_study(data, organization_id, study_id=kf_id)


@click.command()
@click.argument(
    "study_id",
)
@click.option(
    "--disable-delete-safety-check",
    is_flag=True,
    help="This will allow deleting of the organization",
)
def delete_study(study_id, disable_delete_safety_check):
    """
    Delete study in Dewrangle by KF ID or global ID

    \b
    Arguments:
      \b
      node_id - Either a Kids First ID or Dewrangle global ID
    """
    init_logger()

    return gql_client.delete_study(study_id, not disable_delete_safety_check)


@click.command()
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
    init_logger()

    return gql_client.read_studies(output_dir)


@click.command()
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
    init_logger()

    return gql_client.read_study(node_id)
