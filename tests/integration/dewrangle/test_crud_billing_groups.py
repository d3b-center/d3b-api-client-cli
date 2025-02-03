"""
Test CRUD Dewrangle billing_group functions/cmds
"""

import os

import pytest
from click.testing import CliRunner

from d3b_api_client_cli.config import config
from d3b_api_client_cli.cli.dewrangle import (
    upsert_organization,
    create_billing_group,
    read_billing_groups,
    delete_billing_group,
)
from d3b_api_client_cli.dewrangle.graphql import organization

CAVATICA_BILLING_GROUP_ID = config["dewrangle"]["billing_group_id"]


@pytest.fixture(scope="session")
def test_org(organization_file):
    """
    Upsert an Organization in Dewrangle for other tests to use
    """
    fp = organization_file(org_name="Billing Group Tests")
    runner = CliRunner()
    result = runner.invoke(upsert_organization, [fp], standalone_mode=False)
    assert result.exit_code == 0

    yield result.return_value, fp

    organization.delete_organization(
        result.return_value["id"], delete_safety_check=False
    )


def test_crud_billing_group(tmp_path, test_org):
    """
    Test `d3b dewrangle create-billing-group` command
    Test `d3b dewrangle read-billing-groups` command
    Test `d3b dewrangle delete-billing-group` command
    """
    org, _ = test_org

    # Create
    runner = CliRunner()
    result = runner.invoke(
        create_billing_group,
        [
            "--organization-id",
            org["id"],
            "--cavatica-billing-group-id",
            CAVATICA_BILLING_GROUP_ID,
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    billing_group_id = result.return_value["id"]
    assert billing_group_id

    # Can't create same billing group in org
    result = runner.invoke(
        create_billing_group,
        [
            "--organization-id",
            org["id"],
            "--cavatica-billing-group-id",
            CAVATICA_BILLING_GROUP_ID,
        ],
        standalone_mode=False,
    )
    assert result.return_value is None

    # Read
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_billing_groups, ["--output-dir", temp_dir], standalone_mode=False
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "BillingGroup.json"))

    # Delete
    runner = CliRunner()
    result = runner.invoke(
        delete_billing_group,
        [billing_group_id, "--disable-delete-safety-check"],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]
