import os

import pytest
from click.testing import CliRunner

from d3b_api_client_cli.utils import read_json, write_json
from d3b_api_client_cli.config import config, ROOT_DIR, IdTypes
from d3b_api_client_cli.cli import *
from d3b_api_client_cli.dewrangle.graphql import organization


@pytest.fixture(scope="session")
def organization_file(tmp_path_factory):
    """
    Write the inputs to create a Dewrangle Organization to file
    """
    data_dir = tmp_path_factory.mktemp("data")
    org_filepath = os.path.join(data_dir, "Organization.json")
    org = {
        "name": "TestOrg",
        "description": "A test org",
        "visibility": "PRIVATE",
    }
    write_json(org, org_filepath)

    return org_filepath


def test_upsert_organization(organization_file):
    """
    Test `dwds dewrangle upsert-organization` command
    """
    fp = organization_file
    runner = CliRunner()
    result = runner.invoke(upsert_organization, [fp], standalone_mode=False)
    assert result.exit_code == 0
    assert result.return_value["id"]


def test_read_organization(tmp_path):
    """
    Test `dwds dewrangle read-organizations` command
    """
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_organizations, ["--output-dir", temp_dir], standalone_mode=False
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "Organization.json"))


def test_delete_organization():
    """
    Test `dwds dewrangle delete-organization` command
    """
    orgs = organization.read_organizations()
    if not orgs:
        return

    dwid = None
    for org in orgs:
        if org["name"] == "TestOrg":
            dwid = org["id"]
            break

    if not dwid:
        # If there is no TestOrg, the tests must have failed and not created it
        return

    runner = CliRunner()
    result = runner.invoke(
        delete_organization, ["--dewrangle-org-id", dwid], standalone_mode=False
    )
    assert result.exit_code == 0
    assert result.return_value["id"]

    orgs = organization.read_organizations()
    if orgs:
        assert all([org["name"] != "TestOrg" for org in orgs])
