"""
Module: db.postgres.load

This module provides utilities for loading data from files or Pandas DataFrames into
PostgreSQL database tables using SQLAlchemy. It supports batch processing and upserts
using the PostgreSQL `ON CONFLICT` clause.

Features:
- Load CSV data into a PostgreSQL table with automatic table creation.
- Upsert (insert or update on conflict) data using primary keys.
- Support for batch processing to handle large datasets efficiently.
- Column type overrides for customized schema creation.
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional

import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeEngine
import pandas as pd

from d3b_api_client_cli.utils.io import chunked_dataframe_reader
from d3b_api_client_cli.db.postgres import create_sqla_engine, DBConnectionParam

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10000


@dataclass(frozen=True)
class OverrideColumnType:
    """
    Represents a mapping for a column name to a specific SQLAlchemy
        database type.

    This dataclass allows you to define special column types for
    database table creation where a custom SQLAlchemy `TypeEngine`
    is required instead of the default type.

    Attributes:
        column_name (str): The name of the column in the database table.
        db_type (TypeEngine): The SQLAlchemy database type to use for this
            column.
    """

    column_name: str
    db_type: TypeEngine


def save_file_to_db(
    filepath,
    schema_name,
    table_name,
    sqla_engine=None,
    db_conn_args: DBConnectionParam = None,
    batch_size=DEFAULT_BATCH_SIZE,
    dispose_at_end=False,
    upsert_rows=False,
):
    """
    Creates or updates a table within the specified schema and loads data into the table
    from the given file.

    Args:
        filepath (str): Path to the CSV file containing table data.
        schema_name (str): Name of the database schema where the table will be created.
        table_name (str): Name of the database table where data will be upserted.
        batch_size (int): Number of rows to upsert at a time.
        sqla_engine (sqlalchemy.engine.Engine): An existing SQLAlchemy engine instance.
        dispose_at_end (bool, optional): Whether to close the connection after loading. Defaults to False.
        db_conn_args (dict, optional): Additional database connection arguments. Defaults to None.

    Returns:
        None
    """
    logger.info(
        "🗃️ Starting to load %s into %s.%s", filepath, schema_name, table_name
    )

    if not batch_size:
        batch_size = DEFAULT_BATCH_SIZE

    if not sqla_engine:
        # Create connection to db
        sqla_engine = create_sqla_engine(db_conn_args)
        dispose_at_end = True

    filename = os.path.split(filepath)[-1]
    count = 0

    # Stream data from file
    logger.info("Streaming file %s into db table %s...", filename, table_name)
    for _, df in enumerate(chunked_dataframe_reader(filepath, batch_size)):
        # Bulk insert rows into db table
        save_df_to_db(
            df,
            schema_name,
            table_name,
            [],
            sqla_engine,
            upsert_rows=upsert_rows,
        )
        count += df.shape[0]
        logger.info("-- Loaded %d total rows", count)

    if dispose_at_end:
        sqla_engine.dispose()


def save_df_to_db(
    df: pd.DataFrame,
    schema_name: str,
    table_name: str,
    primary_key_columns: list[str],
    col_type_overrides: Optional[list[OverrideColumnType]] = None,
    sqla_engine=None,
    db_conn_args: DBConnectionParam = None,
    dispose_at_end=False,
    upsert_rows=False,
):
    """
    Save a DataFrame into a database table with upsert (insert or update on
        conflict).

    Args:
        df (pd.DataFrame): The DataFrame to load.
        schema_name (str): Name of the database schema.
        table_name (str): Name of the table to load data into.
        primary_key_columns (list[str]): List of primary key column names
            for conflict resolution.
        col_type_overrides (list[OverrideColumnType]): List of types to
            override default SQL type
        sqla_engine (sqlalchemy.engine.Engine, optional): SQLAlchemy engine
            instance. Defaults to None.
        dispose_at_end (bool, optional): Whether to dispose of the engine after
            use. Defaults to False.
        **db_conn_args: DB Connection Args to create an SQLAlchemy engine if
            one is not provided.

    Returns:
        None
    """
    logger.info(
        "🗃️ Starting to load DataFrame into %s.%s", schema_name, table_name
    )

    # Use existing engine or create a new one
    if not sqla_engine:
        sqla_engine = create_sqla_engine(db_conn_args)
        dispose_at_end = True

    # Ensure DataFrame is not empty
    if df.empty:
        logger.warning("⚠️️ The DataFrame is empty. No data to load.")
        if dispose_at_end:
            sqla_engine.dispose()
        return

    with sessionmaker(bind=sqla_engine)() as session:
        # Attempt to create table if it doesn't exists
        table = create_table(
            schema_name,
            table_name,
            primary_key_columns,
            col_type_overrides,
            df.columns,
            sqla_engine,
        )

        # Convert df to dict
        data_to_insert = df.to_dict(orient="records")

        # Create initial Insert Statement
        stmt = insert(table).values(data_to_insert)

        if upsert_rows:
            # Create dict to update on conflicts
            update_columns = {
                col.name: stmt.excluded[col.name] for col in table.columns
            }

            # Add on conflict clause to upsert data
            stmt = stmt.on_conflict_do_update(
                index_elements=primary_key_columns,
                set_=update_columns,
            )

        session.execute(stmt)
        session.commit()

        logger.info("-- Loaded %d total rows", df.shape[0])

    if dispose_at_end:
        sqla_engine.dispose()


def create_table(
    schema_name: str,
    table_name: str,
    primary_key_columns: list[str],
    col_type_overrides: list[OverrideColumnType],
    columns: list[str],
    sqla_engine: sqlalchemy.Engine,
) -> sqlalchemy.Table:
    """
    Create Table in postgres DB if the table doesn't exists.

    Args:
        schema_name (str): The name of the database schema in
            which to create the table.
        table_name (str): The name of the table to create.
        primary_key_columns (list[str]): A list of column names that will serve
          as primary keys for the table.
        col_type_overrides (list[OverrideColumnType]): A list of
            `OverrideColumnType` objects specifying custom SQLAlchemy
            types for specific columns. If a column is not listed here,
            the default (String) type will be used.
        columns (list[str]): A list of all column names
            (including primary key columns).
        sqla_engine (sqlalchemy.Engine): An SQLAlchemy engine instance
            connected to the target database.

    Returns:
        sqlalchemy.Table: The SQLAlchemy `Table` object representing the created
         (or existing) table.

    Behavior:
        - If the table does not exist in the database, it will be created with
            the specified columns and types.
        - Columns listed in `special_col_types` will use the specified
            SQLAlchemy type instead of the default.
        - Primary key columns will be marked accordingly.
    """
    metadata = sqlalchemy.MetaData()

    override_types_map = {
        col.column_name: col.db_type for col in col_type_overrides or []
    }

    # Helper function to get column type
    def get_col_type(col_name) -> TypeEngine:
        default_type = sqlalchemy.String
        return override_types_map.get(col_name, default_type)

    # Define the table schema
    table = sqlalchemy.Table(
        table_name,
        metadata,
        *[
            sqlalchemy.Column(col, get_col_type(col), primary_key=True)
            for col in primary_key_columns
        ],
        *[
            sqlalchemy.Column(col, get_col_type(col))
            for col in columns
            if col not in primary_key_columns
        ],
        schema=schema_name,
    )

    # Create the table in the database if it does not already exist
    metadata.create_all(sqla_engine)

    return table
