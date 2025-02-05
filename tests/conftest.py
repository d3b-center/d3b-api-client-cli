"""
Reusable fixtures and helpers for all tests
"""

import os
import pytest
from click.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from d3b_api_client_cli.utils import write_json

from d3b_api_client_cli.cli import *
from d3b_api_client_cli.dewrangle.graphql import (
    organization,
    study,
    credential,
)
from d3b_api_client_cli.config import config

AWS_ACCESS_KEY_ID = config["aws"]["s3"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = config["aws"]["s3"]["aws_secret_access_key"]
AWS_BUCKET_DATA_TRANSFER_TEST = config["aws"]["s3"]["test_bucket_name"]

POSTGRES_DB_IMAGE = "postgres:16-alpine"


@pytest.fixture(scope="session")
def organization_file(tmp_path_factory):
    """
    Write the inputs to create a Dewrangle Organization to file
    """

    def create_and_write_org(org_name="TestOrg"):
        data_dir = tmp_path_factory.mktemp("data")
        org_filepath = os.path.join(data_dir, "Organization.json")
        org = {
            "name": org_name,
            "description": "A test org",
            "visibility": "PRIVATE",
        }
        write_json(org, org_filepath)

        return org_filepath

    return create_and_write_org


@pytest.fixture(scope="session")
def study_file(tmp_path_factory):
    """
    Write the inputs to create a Dewrangle Organization to file
    """

    def create_and_write_study(study_name="TestStudy", global_id=None):
        data_dir = tmp_path_factory.mktemp("data")
        study_filepath = os.path.join(data_dir, "Study.json")
        study = {
            "name": study_name,
            "description": "A test study",
        }
        if global_id:
            study["global_id"] = global_id
        write_json(study, study_filepath)

        return study_filepath

    return create_and_write_study


@pytest.fixture(scope="session")
def dewrangle_org(organization_file):
    """
    Upsert an Organization in Dewrangle for other tests to use
    """
    fp = organization_file(org_name="Integration Tests")
    runner = CliRunner()
    result = runner.invoke(upsert_organization, [fp], standalone_mode=False)
    assert result.exit_code == 0

    yield result.return_value, fp

    organization.delete_organization(
        result.return_value["id"], delete_safety_check=False
    )


@pytest.fixture(scope="session")
def dewrangle_study(dewrangle_org, study_file):
    """
    Upsert a Dewrangle study into the integration tests org
    """
    org, fp = dewrangle_org
    fp = study_file()

    runner = CliRunner()
    result = runner.invoke(upsert_study, [fp, org["id"]], standalone_mode=False)
    return result.return_value, fp


@pytest.fixture(scope="function")
def delete_credentials(tmp_path):
    """
    Delete all credentials
    """
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()
    credentials = credential.read_credentials(temp_dir)

    for node in credentials.values():
        runner = CliRunner()
        result = runner.invoke(
            delete_credential,
            ["--node-id", node["id"], "--disable-delete-safety-check"],
            standalone_mode=False,
        )
        assert result.exit_code == 0
        assert result.return_value


@pytest.fixture(scope="session")
def dewrangle_credential(dewrangle_study):
    """
    Create credential for dewrangle study
    """
    study, _ = dewrangle_study
    runner = CliRunner()

    result = runner.invoke(
        upsert_credential,
        [
            "--name",
            "e2e",
            "--key",
            AWS_ACCESS_KEY_ID,
            "--secret",
            AWS_SECRET_ACCESS_KEY,
            "--study-id",
            study["id"],
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    credential = result.return_value
    credential["study_global_id"] = study["globalId"]
    credential["study_id"] = study["id"]

    return credential


@pytest.fixture(scope="session")
def make_dewrangle_volume(dewrangle_credential):
    """
    Return a function taht creates a dewrangle volume
    """

    def _make_volume(jira_ticket_number):
        """
        Create a dewrangle volume
        """
        study_id = dewrangle_credential["study_id"]
        runner = CliRunner()
        bucket = AWS_BUCKET_DATA_TRANSFER_TEST
        path_prefix = jira_ticket_number

        # Create
        result = runner.invoke(
            upsert_volume,
            [
                "--bucket",
                bucket,
                "--path-prefix",
                path_prefix,
                "--study-id",
                study_id,
                "--credential-key",
                dewrangle_credential["key"],
            ],
            standalone_mode=False,
        )
        assert result.exit_code == 0

        volume = result.return_value
        volume["study_id"] = study_id
        volume["study_global_id"] = dewrangle_credential["study_global_id"]

        return volume

    return _make_volume


# Postgres DB Fixtures
@pytest.fixture(scope="module")
def postgres_db(request):
    """
    Fixture to create Postgres testcontainer for integration testing
    """
    postgres = PostgresContainer(POSTGRES_DB_IMAGE)
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    os.environ["DB_HOST"] = postgres.get_container_host_ip()
    os.environ["DB_PORT"] = postgres.get_exposed_port(5432)
    os.environ["DB_NAME"] = postgres.dbname
    os.environ["DB_USER"] = postgres.username
    os.environ["DB_USER_PW"] = postgres.password
