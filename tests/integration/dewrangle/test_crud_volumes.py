"""
Test CRUD Dewrangle volume functions/cmds
"""

import os
from pprint import pprint

from click.testing import CliRunner

from d3b_api_client_cli.cli.dewrangle import *
from d3b_api_client_cli.dewrangle.graphql import volume

from d3b_api_client_cli.config import config

AWS_ACCESS_KEY_ID = config["aws"]["s3"]["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = config["aws"]["s3"]["aws_secret_access_key"]
AWS_BUCKET_DATA_TRANSFER_TEST = config["aws"]["s3"]["test_bucket_name"]


def test_upsert_volume_bad_input():
    """
    Test `d3b dewrangle upsert-volume` command
    """
    runner = CliRunner()

    # Create
    result = runner.invoke(
        upsert_volume,
        ["--bucket", "e2e", "--credential-key", "foo"],
        standalone_mode=False,
    )
    assert result.exit_code == 1
    assert "the graphql node ID or global ID" in str(result.exc_info)


def test_crud_volume(tmp_path, dewrangle_credential):
    """
    Test `d3b dewrangle upsert-volume` command
    Test `d3b dewrangle delete-volume` command
    Test `d3b dewrangle read-volumes` command
    """
    study_id = dewrangle_credential["study_id"]
    runner = CliRunner()
    bucket = AWS_BUCKET_DATA_TRANSFER_TEST
    path_prefix = "AD-4000"

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
    assert result.return_value["id"]
    assert result.return_value["name"] == bucket
    assert result.return_value["pathPrefix"] == path_prefix

    # Read
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_volumes,
        [
            "--output-dir",
            temp_dir,
            "--study-global-id",
            dewrangle_credential["study_global_id"],
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "Volume.json"))

    # Update
    result = runner.invoke(
        upsert_volume,
        [
            "--bucket",
            bucket,
            "--path-prefix",
            path_prefix,
            "--credential-key",
            dewrangle_credential["key"],
            "--study-id",
            study_id,
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]
    assert "Update" in result.stdout

    # Delete
    result = runner.invoke(
        delete_volume,
        [
            "--node-id",
            result.return_value["id"],
            "--disable-delete-safety-check",
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]


def test_delete_volume_bad_input():
    """
    Test `d3b dewrangle delete-volume` command
    """
    runner = CliRunner()
    result = runner.invoke(delete_volume, [], standalone_mode=False)
    assert result.exit_code == 1
    assert "must provide" in str(result.exc_info)
