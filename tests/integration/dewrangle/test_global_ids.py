"""
Test Dewrangle global ID commands
"""

import os

import pytest
from click.testing import CliRunner
import pandas

from d3b_api_client_cli.cli.dewrangle.global_id_commands import (
    upsert_global_descriptors,
    download_global_descriptors,
    upsert_and_download_global_descriptors
)
from d3b_api_client_cli.dewrangle.global_id import (
    upsert_global_descriptors as _upsert_global_descriptors
)
from d3b_api_client_cli.faker.global_id import (
    generate_global_id_file,
)


@pytest.fixture(scope="session")
def upserted_global_descriptors(dewrangle_study):
    """
    Upsert global descriptors 
    """
    study, fp = dewrangle_study
    output_dir = os.path.dirname(fp)

    filepath = generate_global_id_file(output_dir=output_dir)

    runner = CliRunner()
    result = runner.invoke(
        upsert_global_descriptors,
        [filepath, "--study-id", study["id"]],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert result.return_value

    return result.return_value, filepath


@pytest.fixture(scope="session")
def downloaded_global_descriptors(upserted_global_descriptors):
    """
    Download newly created global descriptors
    """
    result, filepath = upserted_global_descriptors
    output_dir = os.path.dirname(filepath)
    study_id = result["study_id"]
    job_id = result["job"]["id"]

    runner = CliRunner()

    result = runner.invoke(
        download_global_descriptors,
        [
            "--study-id", study_id, "--job-id", job_id,
            "--output-dir", output_dir
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    filepath = result.return_value

    return study_id, filepath


def test_upsert_global_descriptors(upserted_global_descriptors):
    """
    Test d3b-clients dewrangle upsert-global-descriptors
    """
    upserted_global_descriptors


def test_download_global_descriptors(downloaded_global_descriptors):
    """
    Test d3b-clients dewrangle download-global-descriptors
    """
    _, filepath = downloaded_global_descriptors
    df = pandas.read_csv(filepath)
    assert df.shape[0] == 10


def test_upsert_and_download_global_descriptors(downloaded_global_descriptors):
    """
    Test d3b-clients dewrangle upsert-and-download-global-descriptors
    """
    study_id, filepath = downloaded_global_descriptors
    output_dir = os.path.dirname(filepath)

    # Update the descriptors
    df = pandas.read_csv(filepath)
    df = df[[c for c in ("fhirResourceType", "descriptor", "globalId")]]
    df["descriptor"] = df["descriptor"].apply(
        lambda d: d + "1"
    )
    df.to_csv(filepath, index=False)

    runner = CliRunner()

    # Upsert and download the descriptors
    result = runner.invoke(
        upsert_and_download_global_descriptors,
        [
            filepath,
            "--study-id", study_id,
            "--output-dir", output_dir
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    filepath = result.return_value

    df = pandas.read_csv(filepath)
    assert df.shape[0] == 10


def test_download_all_descriptors(dewrangle_study):
    """
    Test d3b-clients dewrangle download-global-descriptors for all ids
    """
    study, filepath = dewrangle_study
    output_dir = os.path.dirname(filepath)

    runner = CliRunner()
    result = runner.invoke(
        download_global_descriptors,
        [
            "--study-id", study["id"],
            "--download-all",
            "--output-dir", output_dir
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    filepath = result.return_value

    df = pandas.read_csv(filepath)

    # Should have double the descriptors plus one for the study
    assert df.shape[0] == 21
