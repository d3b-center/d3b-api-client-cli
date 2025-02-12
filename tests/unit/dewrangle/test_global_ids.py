"""
Unit test global ID command
"""

import pytest
from click.testing import CliRunner

from d3b_api_client_cli.cli.dewrangle.global_id_commands import (
    upsert_global_descriptors,
)
from d3b_api_client_cli.dewrangle.global_id import (
    upsert_global_descriptors as _upsert_global_descriptors,
    download_global_descriptors as _download_global_descriptors,
)


def test_upsert_global_descriptors_cli_errors():
    """
    Test d3b-clients dewrangle upser-global-descriptor errors
    """
    runner = CliRunner()

    result = runner.invoke(
        upsert_global_descriptors,
        ["global_ids.csv"],
        standalone_mode=False,
    )
    assert result.exit_code == 1
    assert "BadParameter" in str(result.exc_info)
    assert "global ID" in str(result.exc_info)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dewrangle_study_id": None, "study_global_id": "foo"},
        {"dewrangle_study_id": "foo", "study_global_id": None},
    ],
)
def test_upsert_global_descriptors_no_study(mocker, kwargs):
    """
    Test d3b-clients dewrangle upsert-global-descriptors when study
    is not found
    """
    mock_study_api = mocker.patch(
        "d3b_api_client_cli.dewrangle.global_id.study_api"
    )
    mock_study_api.read_study.return_value = {}
    mock_study_api.find_study.return_value = {}

    with pytest.raises(ValueError) as e:
        _upsert_global_descriptors("global_ids.csv", **kwargs)
    assert "does not exist" in str(e)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dewrangle_study_id": None, "study_global_id": "foo"},
        {"dewrangle_study_id": "foo", "study_global_id": None},
    ],
)
def test_download_global_descriptors_no_study(mocker, kwargs):
    """
    Test d3b-clients dewrangle download-global-descriptors when study
    is not found
    """
    mock_study_api = mocker.patch(
        "d3b_api_client_cli.dewrangle.global_id.study_api"
    )
    mock_study_api.read_study.return_value = {}
    mock_study_api.find_study.return_value = {}

    with pytest.raises(ValueError) as e:
        _download_global_descriptors(**kwargs)
    assert "does not exist" in str(e)
