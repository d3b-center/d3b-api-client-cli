"""
Dewrangle volume commands
"""

import logging

import click

from d3b_api_client_cli.config import config
from d3b_api_client_cli.config.log import init_logger
from d3b_api_client_cli.utils import read_json
from d3b_api_client_cli.dewrangle import graphql as gql_client

logger = logging.getLogger(__name__)
DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
AWS_DEFAULT_REGION = config["aws"]["region"]


@click.command()
@click.option(
    "--bucket",
    help="Name of the S3 bucket this Volume points to",
)
@click.option(
    "--path-prefix",
    help="Path in the S3 bucket this Volume points to",
)
@click.option(
    "--region",
    default=AWS_DEFAULT_REGION,
    help="AWS region the S3 bucket of the Volume is in",
)
@click.option(
    "--credential-key",
    required=True,
    help="Credential key, if supplied, must be used with study-global-id"
    " to look up existing volume",
)
@click.option(
    "--study-id",
    help="Graphql node id of the Study this volume belongs to",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study this volume belongs to. If supplied, must be"
    "used with --credential-key to look up existing volume",
)
@click.option(
    "--filepath",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
def upsert_volume(
    bucket,
    path_prefix,
    region,
    credential_key,
    study_id,
    study_global_id,
    filepath,
):
    """
    Upsert volume in Dewrangle. Either provide values in a JSON file or
    provide values via CLI options. If both are provided, CLI options
    take precedence and will overwrite the values from the file

    You can either provide the volume's bucket, path_prefix, and study global
    ID or volume graphql node ID to lookup the volume for an update
    """
    init_logger()

    cli_opts = {
        "name": bucket,
        "pathPrefix": path_prefix,
        "region": region,
    }
    input_data = {}
    if filepath:
        input_data.update(read_json(filepath))
        for k, value in cli_opts.items():
            if value:
                input_data[k] = value
    else:
        input_data = cli_opts

    return gql_client.upsert_volume(
        input_data,
        study_id=study_id,
        study_global_id=study_global_id,
        credential_key=credential_key,
    )


@click.command()
@click.option(
    "--node-id",
    help="Credential graphql node ID",
)
@click.option(
    "--bucket",
    help="Name of the S3 bucket this Volume points to",
)
@click.option(
    "--path-prefix",
    help="Path in the S3 bucket this Volume points to",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study this volume belongs to",
)
@click.option(
    "--disable-delete-safety-check",
    is_flag=True,
    help="This will allow deleting of the organization",
)
def delete_volume(
    node_id, bucket, path_prefix, study_global_id, disable_delete_safety_check
):
    """
    Delete volume

    You can either provide the volume's bucket, path_prefix, and study global
    ID or volume graphql node ID to lookup the volume
    """
    init_logger()

    return gql_client.delete_volume(
        node_id=node_id,
        bucket=bucket,
        path_prefix=path_prefix,
        study_global_id=study_global_id,
        delete_safety_check=not disable_delete_safety_check,
    )


@click.command()
@click.option(
    "--output-dir",
    default=DEWRANGLE_DIR,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="The path to the data dir where volumes will be written",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study to filter volumes by",
)
def read_volumes(output_dir, study_global_id):
    """
    Fetch volumes from Dewrangle
    """
    init_logger()

    return gql_client.read_volumes(
        study_global_id,
        output_dir,
    )


@click.command()
@click.argument(
    "node_id",
)
def get_volume(node_id):
    """
    Get volume in Dewrangle by Dewrangle GraphQL node ID

    \b
    Arguments:
      \b
      node_id - ID of the volume in Dewrangle
    """
    init_logger()

    return gql_client.read_volume(node_id)


@click.command()
@click.option(
    "--volume-id",
    help="Volume graphql node ID",
)
@click.option(
    "--billing-group-id",
    help="Graphql ID of the biling group in Dewrangle",
)
@click.option(
    "--bucket",
    help="Name of the S3 bucket this Volume points to",
)
@click.option(
    "--path-prefix",
    help="Path in the S3 bucket this Volume points to",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study this volume belongs to",
)
def list_and_hash_volume(
    volume_id,
    billing_group_id,
    bucket,
    path_prefix,
    study_global_id,
):
    """
    Trigger a list and hash volume job in Dewrangle

    You can either provide the volume's bucket, path_prefix, and study global
    ID or volume graphql node ID to lookup the volume
    """
    init_logger()

    return gql_client.list_and_hash(
        volume_id=volume_id,
        billing_group_id=billing_group_id,
        bucket=bucket,
        path_prefix=path_prefix,
        study_global_id=study_global_id,
    )


@click.command()
@click.option(
    "--volume-id",
    help="Volume graphql node ID",
)
@click.option(
    "--billing-group-id",
    help="Graphql ID of the biling group in Dewrangle",
)
@click.option(
    "--bucket",
    help="Name of the S3 bucket this Volume points to",
)
@click.option(
    "--path-prefix",
    help="Path in the S3 bucket this Volume points to",
)
@click.option(
    "--study-global-id",
    help="Global ID of the study this volume belongs to",
)
def hash_volume_and_wait(
    volume_id,
    billing_group_id,
    bucket,
    path_prefix,
    study_global_id,
):
    """
    Trigger a list and hash volume job and poll for job status until the
    job is complete or fails

    You can either provide the volume's bucket, path_prefix, and study global
    ID or volume graphql node ID to lookup the volume
    """
    init_logger()

    return gql_client.hash_and_wait(
        volume_id=volume_id,
        billing_group_id=billing_group_id,
        bucket=bucket,
        path_prefix=path_prefix,
        study_global_id=study_global_id,
    )
