"""
Database Postgres Admin Functions
"""

import logging

from psycopg2 import sql, extensions


logger = logging.getLogger(__name__)


def create_db_schema(conn: extensions.connection, schema_name: str) -> None:
    """
    Creates a schema in the database specified by the given connection object.

    Args:
        conn (psycopg2.extensions.connection): An existing psycopg2 connection object.
        schema_name (str): Name of the database schema to create.

    Returns:
        None
    """
    logger.info("✨ Creating new schema %s in database ...", schema_name)
    with conn.cursor() as cursor:
        conn.autocommit = True
        cursor.execute(
            sql.SQL(
                "CREATE SCHEMA {0};",
            ).format(
                sql.Identifier(schema_name),
            )
        )


def create_db_user(
    conn: extensions.connection, user: str, password: str
) -> None:
    """
    Alters an existing database user or creates a new one if it does not exist.

    Args:
        conn (psycopg2.connection): An existing connection object from psycopg2.connect.
        user (str): The username to create or alter.
        password (str): The password for the user.
    """
    logger.info("✨ Upserting user %s in database ...", user)
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


def grant_db_privileges(
    conn: extensions.connection,
    schema_name: str,
    schema_owner: str,
    username: str,
) -> None:
    """
    Grants SELECT privileges to a user on a specified schema.

    Args:
        conn (psycopg2.connection): An existing connection object from psycopg2.connect.
        schema_name (str): The name of the database schema to grant privileges on.
        username (str): The username to grant SELECT privileges to.
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
