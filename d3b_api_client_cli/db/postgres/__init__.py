"""
Database Postgres
"""

import logging
from dataclasses import dataclass
from pprint import pformat

import sqlalchemy

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DBConnectionParam:
    """
    Encapsulates Params needed to establish a DB connection
    """

    username: str
    password: str
    hostname: str
    port: str
    db_name: str

    def __post_init__(self):
        if any(
            not value for value in vars(self).values() if not callable(value)
        ):
            display = {}
            for k, v in vars(self).items():
                display[k] = "*" * len(v) if ("password" in k) and v else v
            raise ValueError(
                "❌ Not enough inputs to connect to database!\n"
                f"{pformat(display)}"
            )


def create_sqla_engine(db_conn: DBConnectionParam) -> sqlalchemy.Engine:
    """
    Creates and returns an SQLAlchemy Engine for connecting to a PostgreSQL database.

    Args:
        db_conn_args (DBConnectionParam): A dictionary containing the database connection parameters:
            - "username" (str): The database username.
            - "password" (str): The database password.
            - "host" (str): The database hostname or IP address.
            - "port" (int): The port number for the database connection.
            - "db_name" (str): The name of the database.

        Returns:
            sqlalchemy.Engine: An SQLAlchemy Engine instance for interacting with the database.

        Raises:
            KeyError: If any required key is missing from `db_conn_args`.
    """
    # Create connection to db
    return sqlalchemy.create_engine(
        f"postgresql://{db_conn.username}:{db_conn.password}@{db_conn.hostname}:{db_conn.port}/{db_conn.db_name}",
        connect_args={"connect_timeout": 5},
    )
