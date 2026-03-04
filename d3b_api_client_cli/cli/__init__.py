"""
Entrypoint for the CLI

All commands are initialized here
"""

from d3b_api_client_cli.cli.dewrangle.graphql_commands import *
from d3b_api_client_cli.cli.dewrangle.ingest_commands import *
from d3b_api_client_cli.cli.dewrangle.setup_commands import *
from d3b_api_client_cli.cli.dewrangle.global_id_commands import *
from d3b_api_client_cli.cli.fhir.commands import *
from d3b_api_client_cli.cli.postgres import *
from d3b_api_client_cli.cli.faker import *


@click.group()
def faker():
    """
    Group of lower level CLI commands related to generating fake data
    """


@click.group()
def postgres():
    """
    Group of lower level CLI commands related to working with Postgres DB
    """


@click.group()
def dewrangle():
    """
    Group of CLI commands relating to working with the Dewrangle
    """
    pass


@click.group()
def fhir():
    """
    Group of CLI commands relating to FHIR service operations
    """
    pass


@click.group()
@click.version_option()
def main():
    """
    A CLI tool for runninng functions necessary for data transfers in D3b.

    This method does not need to be implemented. cli is the root group that all
    subcommands will implicitly be part of.
    """


# Fake data commands
faker.add_command(generate_global_id_file)

# Postgres API commands
postgres.add_command(save_file_to_db)

# CRUD Graphql commands
dewrangle.add_command(upsert_organization)
dewrangle.add_command(delete_organization)
dewrangle.add_command(read_organizations)
dewrangle.add_command(upsert_study)
dewrangle.add_command(delete_study)
dewrangle.add_command(read_studies)
dewrangle.add_command(get_study)
dewrangle.add_command(read_fhir_ingest_job)
dewrangle.add_command(read_fhir_servers)
dewrangle.add_command(upsert_fhir_server)
dewrangle.add_command(delete_fhir_server)

# Ingest into Dewrangle commands
dewrangle.add_command(upload_study_file)
dewrangle.add_command(ingest_study_file)
dewrangle.add_command(ingest_study_files)

# Setup commands
dewrangle.add_command(setup_dewrangle_org)
dewrangle.add_command(setup_dewrangle_study)
dewrangle.add_command(setup_all_studies)

# Global ID commands
dewrangle.add_command(upsert_global_ids)
dewrangle.add_command(download_global_ids)

# FHIR commands
fhir.add_command(build)
fhir.add_command(load_fhir)
fhir.add_command(delete_from_file)
fhir.add_command(delete_all)
fhir.add_command(delete_fhir_study)
fhir.add_command(total_counts)
