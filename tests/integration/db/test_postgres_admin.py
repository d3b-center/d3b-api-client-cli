import os
import pytest

import pandas as pd
from psycopg2 import connect, sql

from d3b_api_client_cli.db.postgres.admin import (
    create_db_schema,
    create_db_user,
)


TEST_SCHEMA = "TEST_SCHEMA"
TEST_USER = "test_user"
TEST_PASSWORD = "p@ssword1"


def test_create_db_schema(postgres_db):
    """
    Test Create DB Schema
    """
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_USER_PW")

    conn = connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )

    create_db_schema(conn, TEST_SCHEMA)
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(
            sql.SQL(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;",
            ),
            [TEST_SCHEMA],
        )
        assert len(cursor.fetchall()) == 1


def test_create_db_user(postgres_db):
    """
    Test Create User in DB
    """

    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_USER_PW")

    conn = connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )
    create_db_user(conn, TEST_USER, TEST_PASSWORD)
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(
            sql.SQL(
                "SELECT 1 FROM pg_roles WHERE rolname = {user_literal}",
            ).format(user_literal=sql.Literal(TEST_USER)),
        )
        assert len(cursor.fetchall()) == 1
