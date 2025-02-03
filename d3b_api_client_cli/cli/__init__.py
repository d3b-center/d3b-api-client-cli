"""
Entrypoint for the CLI

All commands are initialized here
"""

import click
from d3b_api_client_cli.cli.dewrangle import *


@click.group()
def dewrangle():
    """
    Group of lower level CLI commands relating to working directly with the
    Dewrangle API
    """


@click.group()
@click.version_option()
def main():
    """
    A CLI tool for runninng functions necessary for data transfers in D3b.

    This method does not need to be implemented. cli is the root group that all
    subcommands will implicitly be part of.
    """


# Dewrangle API commands
dewrangle.add_command(upsert_organization)
dewrangle.add_command(delete_organization)
dewrangle.add_command(read_organizations)
dewrangle.add_command(upsert_study)
dewrangle.add_command(delete_study)
dewrangle.add_command(read_studies)
dewrangle.add_command(upsert_credential)
dewrangle.add_command(delete_credential)
dewrangle.add_command(read_credentials)
dewrangle.add_command(upsert_volume)
dewrangle.add_command(delete_volume)
dewrangle.add_command(read_volumes)
dewrangle.add_command(list_and_hash_volume)
dewrangle.add_command(hash_volume_and_wait)
dewrangle.add_command(read_job)
dewrangle.add_command(create_billing_group)
dewrangle.add_command(delete_billing_group)
dewrangle.add_command(read_billing_groups)

# Add command groups to the root CLI
main.add_command(dewrangle)
