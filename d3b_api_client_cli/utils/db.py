"""
Database utility functions
"""

import os
import time
import logging

from psycopg2 import sql, connect
import psycopg2.extras
import sqlalchemy

from d3b_api_client_cli.utils.data import chunked_dataframe_reader
from d3b_api_client_cli.utils.misc import elapsed_time_hms

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10000


def load_table_from_file(
    filepath,
    schema_name,
    table_name,
    batch_size=DEFAULT_BATCH_SIZE,
    sqla_engine=None,
    dispose_at_end=False,
    **db_conn_args,
):
    """
    Create/update a table within the specified schema and load the data in
    into the specified table with the data from the file

    :param filepath: Path to csv file containing table data
    :type filepath: str
    :param schema_name: Name of db schema where tables get created
    :type schema_name: str
    :param table_name: Name of db table where table will be upserted
    :type table_name: str
    :param batch_size: Number of rows to upsert at a time
    :type batch_size: int
    :param sqla_engine: Existing sqlalchemy engine instance
    :type sqla_engine: sqlalchemy.engine.Engine
    :param dispose_at_end:  Whether to close the connection after load
    :type dispose_at_end: boolean
    :param db_conn_args:
    :type db_conn_args:
    """
    logger.info(
        f"🗃️ Starting to load {filepath} into {schema_name}.{table_name}"
    )
    start_time = time.time()

    if not sqla_engine:
        # Create connection to db
        try:
            username = db_conn_args["username"]
            password = db_conn_args["password"]
            hostname = db_conn_args["host"]
            port = db_conn_args["port"]
            db_name = db_conn_args["db_name"]
        except KeyError:
            logger.error("❌ Not enough inputs to connect to database!")
            raise

        sqla_engine = sqlalchemy.create_engine(
            f"postgresql://{username}:{password}@{hostname}:{port}/{db_name}",
            connect_args={"connect_timeout": 5},
        )
        dispose_at_end = True

    filename = os.path.split(filepath)[-1]
    count = 0

    # Stream data from file
    logger.info(f"Streaming file {filename} into db table {table_name}...")
    for _, df in enumerate(chunked_dataframe_reader(filepath)):
        # Bulk insert rows into db table
        df.to_sql(
            table_name,
            sqla_engine,
            schema=schema_name,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=batch_size,
        )
        count += df.shape[0]
        logger.info(f"-- Loaded {count} total rows")

    if dispose_at_end:
        sqla_engine.dispose()

    end_time = elapsed_time_hms(start_time)
    logger.info(f"⏰ Elapsed time (hh:mm:ss): {end_time}")


def create_db_schema(conn, schema_name):
    """
    Create schema in db specified by the conn Connection object

    :param conn: an existing psycopg2 connection
    :type conn: resulting object from psycopg2.connect
    :param schema_name: Name of db schema to create
    :type schema_name: str
    """
    logger.info(f"✨ Creating new schema {schema_name} in database ...")
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(
            sql.SQL(
                "CREATE SCHEMA {0};",
            ).format(
                sql.Identifier(schema_name),
            )
        )


def create_db_user(conn, user, password):
    """
    Alter db user if it exists
    Create a new db user if it does not exist

    :param conn: an existing psycopg2 connection
    :type conn: resulting object from psycopg2.connect
    :param user: username to create
    :type user: str
    :param user: password to create
    :type user: str
    """
    logger.info(f"✨ Upserting user {user} in database ...")
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(
            sql.SQL(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1
                    FROM pg_roles
                    WHERE rolname = {user_literal}
                  ) THEN
                    CREATE USER {user} WITH
                    CONNECTION LIMIT 1000
                    LOGIN ENCRYPTED PASSWORD {password};
                  ELSE
                    ALTER USER {user} WITH
                    ENCRYPTED PASSWORD {password};
                  END IF;
                END
                $$;
                """
            ).format(
                user_literal=sql.Literal(user),
                user=sql.Identifier(user),
                password=sql.Literal(password),
            )
        )


def grant_db_privileges(conn, schema_name, schema_owner, username):
    """
    Grant select privileges to user on schema

    :param conn: an existing psycopg2 connection
    :type conn: resulting object from psycopg2.connect
    :param schema_name: Name of db schema to grant privileges to
    :type schema_name: str
    :param username: username to grant select privileges to
    :type username: str
    """
    with conn.cursor() as cursor:
        conn.autocommit = True
        query = sql.SQL(
            "GRANT USAGE ON SCHEMA {schema_name} TO {username};"
            "ALTER DEFAULT PRIVILEGES FOR USER {schema_owner} IN SCHEMA "
            "{schema_name} "
            "GRANT ALL ON TABLES TO {username};"
        ).format(
            username=sql.Identifier(username),
            schema_owner=sql.Identifier(schema_owner),
            schema_name=sql.Identifier(schema_name),
        )
        cursor.execute(query)


def run_query(query_string, username, password, dbname, host, port):
    """
    Run a basic sql query with no variables
    """

    try:
        with connect(
            dbname=dbname,
            user=username,
            password=password,
            host=host,
            port=port,
        ) as conn:
            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query_string)
                rows = cursor.fetchall()
    except psycopg2.OperationalError as e:
        logger.error(
            "❌ Could not execute query due to failure to connect to db:"
            f" {host}:{port}/{dbname}"
            " Check your connection details in the environment"
        )
        raise e

    return rows
