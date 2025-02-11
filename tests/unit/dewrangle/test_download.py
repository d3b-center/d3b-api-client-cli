"""
Test downloading volume hash files (job errors) from Dewrangle
"""

import os
import requests_mock
import pytest

from d3b_api_client_cli.dewrangle.rest import files
from d3b_api_client_cli.config import config

DEWRANGLE_BASE_URL = config["dewrangle"]["base_url"]


def test_filename_from_headers():
    """
    Test helper function to extract filename from http response headers
    """
    headers = {"Content-Disposition": "attachment; filename=file.csv"}
    filename = files._filename_from_headers(headers)
    assert filename == "file.csv"


def test_download_file_original_filename(tmp_path):
    """
    Test download file from Dewrangle with original filename
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    url = "https://dewrangle.com/files"
    expected_filename = "volume_hash_file.csv"
    headers = {
        "Content-Disposition": f"attachment; filename={expected_filename}"
    }
    with requests_mock.Mocker() as m:
        # Setup mock
        m.get(url, content=b"foo", headers=headers)
        filepath = files.download_file(url, output_dir=output_dir)

        _, filename = os.path.split(filepath)
        assert filename == expected_filename
        assert os.path.isfile(os.path.join(output_dir, expected_filename))


def test_download_file_to_filepath(tmp_path):
    """
    Test download file from Dewrangle to provided filepath
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    expected_filename = "foo.csv"
    expected_filepath = os.path.join(output_dir, expected_filename)

    url = "https://dewrangle.com/files"
    headers = {"Content-Disposition": "attachment; filename=dewrangle.csv"}
    with requests_mock.Mocker() as m:
        # Setup mock
        m.get(url, content=b"foo", headers=headers)
        filepath = files.download_file(url, filepath=expected_filepath)

        _, filename = os.path.split(filepath)
        assert filename == expected_filename
        assert os.path.isfile(expected_filepath)


def test_download_job_errors(mocker):
    """
    Test download Dewrangle job errors
    """
    mock_download_file = mocker.patch(
        "d3b_api_client_cli.dewrangle.rest.files.download_file"
    )

    files.download_job_errors("job-id", output_dir="output")

    endpoint_template = config["dewrangle"]["endpoints"]["rest"]["job_errors"]
    endpoint = endpoint_template.format(job_id="job-id")
    url = f"{DEWRANGLE_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"

    mock_download_file.assert_called_with(
        url, output_dir="output", filepath=None
    )


@pytest.mark.parametrize(
    "download_method", [files.download_job_errors]
)
@pytest.mark.parametrize(
    "token,url, expected_msg",
    [
        (None, None, "Missing required configuration"),
        ("foo", None, "Missing required configuration"),
        (None, "foo", "Missing required configuration"),
    ],
)
def test_missing_configuration(
    mocker, download_method, token, url, expected_msg
):
    """
    Test download files with missing configuration
    """
    mocker.patch("d3b_api_client_cli.config.DEWRANGLE_DEV_PAT", token)
    mocker.patch("d3b_api_client_cli.config.DEWRANGLE_BASE_URL", url)

    if expected_msg:
        with pytest.raises(ValueError) as e:
            download_method("1234")
        assert expected_msg in str(e)
    else:
        assert download_method("1234")
