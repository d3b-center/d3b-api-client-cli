"""
Test CRUD Dewrangle credential functions/cmds
"""

import os
from pprint import pprint

from click.testing import CliRunner

from d3b_api_client_cli.cli.dewrangle import *
from d3b_api_client_cli.dewrangle.graphql import credential

from d3b_api_client_cli.config import config

AWS_ACCESS_KEY_ID = config["aws"]["s3"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = config["aws"]["s3"]["aws_secret_access_key"]


def test_upsert_credential_bad_input():
    """
    Test `d3b-clients dewrangle upsert-credential` command
    """
    runner = CliRunner()

    # Create
    result = runner.invoke(
        upsert_credential,
        [
            "--name",
            "e2e",
            "--key",
            AWS_ACCESS_KEY_ID,
            "--secret",
            AWS_SECRET_ACCESS_KEY,
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 1
    assert "the graphql node ID or global ID" in str(result.exc_info)


def test_crud_credential(tmp_path, dewrangle_study):
    """
    Test `d3b-clients dewrangle upsert-credential` command
    Test `d3b-clients dewrangle delete-credential` command
    Test `d3b-clients dewrangle read-credentials` command
    """
    study, _ = dewrangle_study
    runner = CliRunner()

    # Create
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
    assert result.return_value["id"]
    assert result.return_value["name"] == "e2e"

    # Read
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_credentials,
        [
            "--output-dir",
            temp_dir,
            "--study-global-id",
            study["globalId"],
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "Credential.json"))

    # Update
    result = runner.invoke(
        upsert_credential,
        [
            "--name",
            "foobar",
            "--key",
            AWS_ACCESS_KEY_ID,
            "--secret",
            AWS_SECRET_ACCESS_KEY,
            "--study-global-id",
            study["globalId"],
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]
    assert result.return_value["name"] == "foobar"

    # Delete
    result = runner.invoke(
        delete_credential,
        [
            "--node-id",
            result.return_value["id"],
            "--disable-delete-safety-check",
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]


def test_delete_credential_bad_input():
    """
    Test `d3b-clients dewrangle delete-credential` command
    """
    runner = CliRunner()
    result = runner.invoke(delete_credential, [], standalone_mode=False)
    assert result.exit_code == 1
    assert "must provide" in str(result.exc_info)
