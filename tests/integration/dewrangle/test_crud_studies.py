"""
Test CRUD Dewrangle study functions/cmds
"""

import os
from pprint import pprint

from click.testing import CliRunner

from d3b_api_client_cli.cli.dewrangle import *
from d3b_api_client_cli.dewrangle.graphql import study


def test_crud_study(tmp_path, dewrangle_study):
    """
    Test `d3b dewrangle upsert-study` command
    Test `d3b dewrangle read-studies` command
    Test `d3b dewrangle delete-study` command
    """
    study_obj, fp = dewrangle_study

    # Update
    runner = CliRunner()
    result = runner.invoke(
        upsert_study, [fp, study_obj["organization_id"]], standalone_mode=False
    )
    study_id = result.return_value["id"]
    assert result.exit_code == 0
    assert study_id

    # Read
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_studies, ["--output-dir", temp_dir], standalone_mode=False
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "Study.json"))

    # Delete
    runner = CliRunner()
    result = runner.invoke(
        delete_study,
        [study_id, "--disable-delete-safety-check"],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["id"]
