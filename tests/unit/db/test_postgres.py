"""
Unit tests for interacting with postgres
"""
import pytest

from d3b_api_client_cli.db.postgres import DBConnectionParam


def test_db_connection_params():
    """
    Test DBConnectionParam
    """
    username = "foo"
    password = "bar"
    hostname = "postgres.com"
    port = "5432"
    db_name = "postgres"

    params = DBConnectionParam(
        username=username,
        password=password,
        hostname=hostname,
        port=port,
        db_name=db_name
    )

    assert params.username == username
    assert params.password == password
    assert params.hostname == hostname
    assert params.port == port
    assert params.db_name == db_name


def test_db_connection_params_error():
    """
    Test DBConnectionParam errors
    """
    username = ""
    password = ""
    hostname = None
    port = None
    db_name = "postgres"

    with pytest.raises(ValueError):
        DBConnectionParam(
            username=username,
            password=password,
            hostname=hostname,
            port=port,
            db_name=db_name
        )
