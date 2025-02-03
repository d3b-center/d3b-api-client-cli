"""
Test Dewrangle organization cmds
"""

import os

import pytest
from click.testing import CliRunner

from d3b_api_client_cli.utils import read_json, write_json
from d3b_api_client_cli.cli import *
from d3b_api_client_cli.dewrangle.graphql import organization


def test_upsert_organization(tmp_path, organization_file):
    """
    Test `d3b-clients dewrangle upsert-organization` command
    """
    # Create
    fp = organization_file()
    organization = read_json(fp)
    runner = CliRunner()
    result = runner.invoke(upsert_organization, [fp], standalone_mode=False)
    assert result.exit_code == 0
    assert result.return_value["id"]

    # Update
    organization["name"] = "foobar"
    write_json(organization, fp)
    result = runner.invoke(upsert_organization, [fp], standalone_mode=False)
    assert result.exit_code == 0
    assert result.return_value["name"] == "foobar"

    # Delete it
    result = runner.invoke(
        delete_organization,
        [
            "--dewrangle-org-id",
            result.return_value["id"],
            "--disable-delete-safety-check",
        ],
        standalone_mode=False,
    )


def test_read_organization(tmp_path):
    """
    Test `d3b-clients dewrangle read-organizations` command
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


def test_delete_organization_safety_check_on():
    """
    Test `d3b-clients dewrangle delete-organization` command
    with safety check enabled for delete
    """
    orgs = organization.read_organizations()
    if not orgs:
        return

    runner = CliRunner()
    result = runner.invoke(
        delete_organization,
        ["--dewrangle-org-name", "TestOrg"],
        standalone_mode=False,
    )
    assert result.exit_code == 1
    assert "DELETE_SAFETY_CHECK" in str(result.exc_info)

    orgs = organization.read_organizations()
    found_org = None
    if orgs:
        for org in orgs:
            if org["name"] == "TestOrg":
                found_org = org
                break
        assert found_org


def test_delete_organization_safety_check_off():
    """
    Test `d3b-clients dewrangle delete-organization` command
    with safety check disabled for delete
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
        delete_organization,
        ["--dewrangle-org-id", dwid, "--disable-delete-safety-check"],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]

    orgs = organization.read_organizations()
    if orgs:
        assert all([org["name"] != "TestOrg" for org in orgs])
