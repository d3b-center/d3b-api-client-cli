import os
import pytest

import pandas as pd
from psycopg2 import connect, sql

from d3b_api_client_cli.db.postgres.save import save_df_to_db
from d3b_api_client_cli.db.postgres import DBConnectionParam

TEST_SCHEMA = "TEST_SCHEMA"
TEST_USER = "test_user"
TEST_PASSWORD = "p@ssword1"
TOTAL_FILES_TO_GENERATE = 10
TEST_JIRA_TICKET_NUMBER = "AD_1234"

TEST_COL_1 = [1, 2, 3]
TEST_COL_1_NAME = "test_col1"
TEST_COL_2 = ["a", "b", "c"]
TEST_COL_2_NAME = "test_col2"


@pytest.fixture
def test_dataframe():
    """
    Generate test dataframe
    """
    data = {TEST_COL_1_NAME: TEST_COL_1, TEST_COL_2_NAME: TEST_COL_2}
    return pd.DataFrame(data)


def test_df_to_db(postgres_db, test_dataframe):
    """
    Test Load Table From DF
    """

    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_USER_PW")

    db_params = DBConnectionParam(
        db_user, db_password, db_host, db_port, db_name
    )

    save_df_to_db(
        test_dataframe,
        TEST_SCHEMA,
        "test_table",
        primary_key_columns=[],
        db_conn_args=db_params,
    )

    conn = connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )

    query = sql.SQL(
        "SELECT * FROM {0}.test_table;",
    ).format(sql.Identifier(TEST_SCHEMA))
    df_db = pd.read_sql_query(query.as_string(conn), conn)
    assert df_db.shape[0] == 3
