import os

import pytest
from click.testing import CliRunner

from d3b_api_client_cli.utils import read_json, kf_id_to_global_id
from d3b_api_client_cli.config import (
    config,
    ROOT_DIR,
    IdTypes,
    TEST_STUDY_ID,
)
from d3b_api_client_cli.cli import *
from d3b_api_client_cli.dewrangle.graphql import study


def test_upsert_study(dewrangle_org):
    """
    Test `dwds dewrangle upsert-study` command
    """
    study_id = TEST_STUDY_ID

    fp = os.path.join(ROOT_DIR, "tests/data/test-study.json")
    runner = CliRunner()
    result = runner.invoke(
        upsert_study, [fp, dewrangle_org["id"]], standalone_mode=False
    )
    assert result.exit_code == 0
    assert result.return_value["id"]


def test_read_studies(tmp_path):
    """
    Test `dwds dewrangle read-studies` command
    """
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_studies, ["--output-dir", temp_dir], standalone_mode=False
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "Study.json"))


def test_delete_study():
    """
    Test `dwds dewrangle delete-study` command
    """
    runner = CliRunner()
    result = runner.invoke(
        delete_study,
        [TEST_STUDY_ID, "--id-type", IdTypes.KIDS_FIRST.value],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value

    studies = study.read_studies()
    assert all(
        [
            study["globalId"] != kf_id_to_global_id(TEST_STUDY_ID)
            for study in studies.values()
        ]
    )
