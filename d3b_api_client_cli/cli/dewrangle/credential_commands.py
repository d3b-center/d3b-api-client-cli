"""
Dewrangle credential commands
"""

import logging

import click

from d3b_api_client_cli.config import config
from d3b_api_client_cli.config.log import init_logger
from d3b_api_client_cli.utils import read_json
from d3b_api_client_cli.dewrangle import graphql as gql_client

logger = logging.getLogger(__name__)
DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
DEFAULT_CREDENTIAL_TYPE = config["dewrangle"]["credential_type"]


@click.command()
@click.option(
    "--name",
    help="Credential name",
)
@click.option(
    "--key",
    help="Credential key",
)
@click.option(
    "--secret",
    help="Credential secret",
)
@click.option(
    "--credential-type",
    default=DEFAULT_CREDENTIAL_TYPE,
    help="Credential name",
)
@click.option(
    "--study-id",
    help="Graphql node id of the Study this credential belongs to",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study this credential belongs to",
)
@click.option(
    "--filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
def upsert_credential(
    name,
    key,
    secret,
    credential_type,
    study_id,
    study_global_id,
    filepath,
):
    """
    Upsert credential in Dewrangle. Either provide values in a JSON file or
    provide values via CLI options. If both are provided, CLI options
    take precedence and will overwrite the values from the file
    """
    init_logger()

    cli_opts = {
        "key": key,
        "secret": secret,
        "name": name,
        "type": credential_type,
    }
    input_data = {}
    if filepath:
        input_data.update(read_json(filepath))
        for k, value in cli_opts.items():
            if value:
                input_data[k] = value
    else:
        input_data = cli_opts

    return gql_client.upsert_credential(
        input_data, study_id=study_id, study_global_id=study_global_id
    )


@click.command()
@click.option(
    "--node-id",
    help="Credential graphql node ID",
)
@click.option(
    "--credential-key",
    help="Credential key",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study this credential belongs to",
)
@click.option(
    "--disable-delete-safety-check",
    is_flag=True,
    help="This will allow deleting of the organization",
)
def delete_credential(
    node_id, credential_key, study_global_id, disable_delete_safety_check
):
    """
    Delete credential using either credential key or credential graphql node
    ID
    """
    init_logger()

    return gql_client.delete_credential(
        node_id=node_id,
        credential_key=credential_key,
        study_global_id=study_global_id,
        delete_safety_check=not disable_delete_safety_check,
    )


@click.command()
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where credentials will be written",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study to filter credentials by",
)
def read_credentials(output_dir, study_global_id):
    """
    Fetch credentials from Dewrangle
    """
    init_logger()

    return gql_client.read_credentials(
        study_global_id,
        output_dir,
    )


@click.command()
@click.argument(
    "node_id",
)
def get_credential(node_id):
    """
    Get credential in Dewrangle by Dewrangle GraphQL node ID

    \b
    Arguments:
      \b
      node_id - ID of the credential in Dewrangle
    """
    init_logger()

    return gql_client.read_credential(node_id)
