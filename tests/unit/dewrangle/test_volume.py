"""
Test Dewrangle volume related functionality
"""

import pytest
from d3b_api_client_cli.dewrangle.graphql.volume import list_and_hash


def test_list_and_hash(mocker):
    """
    Test volume.list_and_hash
    """
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.volume.find_study",
        return_value={"id": "study"},
    )
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.volume.find_volume",
        return_value={"id": "volume"},
    )
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.volume.exec_query",
        return_value={"volumeListAndHash": {"job": {"id": "foo"}}},
    )
    result = list_and_hash("billing", bucket="vol", study_global_id="study")
    assert result["id"]


def test_list_and_hash_missing_inputs():
    """
    Test volume.list_and_hash with missing inputs to lookup volume
    """

    with pytest.raises(ValueError) as e:
        list_and_hash("billing", bucket="vol")
    assert "must provide" in str(e)

    with pytest.raises(ValueError) as e:
        list_and_hash("billing", study_global_id="study")
    assert "must provide" in str(e)


def test_list_and_hash_no_billing_group(mocker):
    """
    Test volume.list_and_hash empty billing group
    """
    with pytest.raises(ValueError) as e:
        list_and_hash("", bucket="vol", study_global_id="study")
    assert "Billing group" in str(e)


def test_list_and_hash_no_study(mocker):
    """
    Test volume.list_and_hash without existing study
    """
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.volume.find_study",
        return_value={},
    )

    with pytest.raises(ValueError) as e:
        list_and_hash("billing", bucket="vol", study_global_id="study")
    assert "study with ID" in str(e)


def test_list_and_hash_no_volume(mocker):
    """
    Test volume.list_and_hash without existing volume
    """
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.volume.find_study",
        return_value={"id": "study"},
    )
    mocker.patch(
        "d3b_api_client_cli.dewrangle.graphql.volume.find_volume",
        return_value={},
    )

    with pytest.raises(ValueError) as e:
        list_and_hash("billing", bucket="vol", study_global_id="study")
    assert "volume with ID" in str(e)
