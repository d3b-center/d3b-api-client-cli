import pytest

from d3b_api_client_cli.dewrangle.graphql.common import create_graphql_client


@pytest.mark.parametrize(
    "token,url, expected_msg",
    [
        (None, None, "Missing required configuration"),
        ("foo", None, "Missing required configuration"),
        (None, "foo", "Missing required configuration"),
        ("bar", "foo", None),
    ],
)
def test_missing_configuration(mocker, token, url, expected_msg):
    """
    Test create_graphql_client with missing configuration
    """
    mocker.patch("d3b_api_client_cli.config.DEWRANGLE_DEV_PAT", token)
    mocker.patch("d3b_api_client_cli.config.DEWRANGLE_BASE_URL", url)

    if expected_msg:
        with pytest.raises(ValueError) as e:
            create_graphql_client()
        assert expected_msg in str(e)
    else:
        assert create_graphql_client()
