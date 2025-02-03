"""
Test job related functionality

Poll job
"""

import os
import time
import pytest
from click.testing import CliRunner

from d3b_api_client_cli.dewrangle.graphql import job
from d3b_api_client_cli.cli import read_job


@pytest.mark.parametrize(
    "status,expected_exc",
    [
        ({"complete": True, "success": True}, None),
        ({"done": True, "success": True}, ValueError),
    ],
)
def test_validate_status_format(status, expected_exc):
    """
    Test function that validates the complete function,
    called after polling completes, returned a valid result
    """
    if expected_exc:
        with pytest.raises(expected_exc):
            job._validate_status_format(status)
    else:
        job._validate_status_format(status)


def test_poll_job_success(mocker):
    """
    Test polling Dewrangle for job status on successful completion
    """
    # Test success case
    mock_results = [
        {
            "node": {
                "id": "foo",
                "operation": "VOLUME_LIST_AND_HASH",
                "completedAt": None,
                "errors": {"edges": []},
            }
        }
        for i in range(3)
    ]
    mock_results[-1]["node"]["completedAt"] = "completed date"
    mock_exec_query = mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.job.exec_query",
        side_effect=mock_results,
    )

    output = job.poll_job("job_id")

    assert output["success"] == True
    assert mock_exec_query.call_count == 3


def test_poll_job_errors(mocker):
    """
    Test polling Dewrangle for job status with errors
    """
    # Test success case
    mock_results = [
        {
            "node": {
                "id": "foo",
                "operation": "VOLUME_LIST_AND_HASH",
                "completedAt": None,
                "errors": {"edges": []},
            }
        }
        for i in range(3)
    ]
    mock_results[-1]["node"]["errors"] = {
        "edges": [{"message": "something went wrong"}]
    }
    mock_exec_query = mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.job.exec_query",
        side_effect=mock_results,
    )

    output = job.poll_job("job_id")

    assert output["success"] is False
    assert output["job"]["errors"]["edges"]
    assert mock_exec_query.call_count == 3


def test_poll_job_timeout(mocker):
    """
    Test polling Dewrangle for job status with a timeout
    """

    def mock_func(query, variables):
        time.sleep(2)
        return {
            "node": {
                "id": "foo",
                "operation": "VOLUME_LIST_AND_HASH",
                "completedAt": None,
                "errors": {"edges": []},
            }
        }

    mock_exec_query = mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.job.exec_query",
        side_effect=mock_func,
    )

    output = job.poll_job("job_id", timeout_seconds=1)

    assert output["success"] is None
    assert mock_exec_query.call_count == 1


def test_read_job(tmp_path, mocker):
    """
    Test d3b dewrangle read-job command
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    mock_results = [
        {
            "node": {
                "id": "foo",
                "operation": "VOLUME_LIST_AND_HASH",
                "completedAt": None,
                "errors": {"edges": []},
            }
        }
    ]
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.job.exec_query",
        side_effect=mock_results,
    )
    runner = CliRunner()
    result = runner.invoke(
        read_job, ["job-id", "--output-dir", output_dir], standalone_mode=False
    )
    assert result.exit_code == 0
    assert os.path.isfile(
        os.path.join(output_dir, "Job-volume-list-and-hash.json")
    )


def test_read_job_errors(mocker):
    """
    Test d3b dewrangle read-job command
    """
    mock_results = [
        {
            "node": {
                "id": "foo",
                "operation": "VOLUME_LIST_AND_HASH",
                "completedAt": None,
                "errors": {"edges": [{"message": "bad"}]},
            }
        }
    ]
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.job.exec_query",
        side_effect=mock_results,
    )
    runner = CliRunner()
    result = runner.invoke(
        read_job,
        [
            "job-id",
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value["errors"]["edges"]
