import logging

import click

from d3b_api_client_cli.config.log import init_logger
from d3b_api_client_cli.config import (
    DB_HOST,
    DB_NAME,
    DB_PORT,
    DB_USER,
    DB_USER_PW,
)
from d3b_api_client_cli.db.postgres.save import (
    save_file_to_db as _save_file_to_db,
)
from d3b_api_client_cli.db.postgres import DBConnectionParam

logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "filepath",
    type=click.Path(exists=True, file_okay=True),
)
@click.argument("schema_name")
@click.argument("table_name")
@click.option(
    "--upsert-rows",
    is_flag=True,
    show_default=True,
    default=False,
    help="Enable Upsert to overwrite rows in Postgres DB",
)
@click.option(
    "--batch-size",
    required=False,
    help="Number of rows to read from the file in one iteration",
)
def save_file_to_db(filepath, schema_name, table_name, upsert_rows, batch_size):
    """
    Save file to Postgres DB

    \b
    Arguments:
        \b
        filepath - Path to CSV file to save to the database.
        schema_name - Name of the database schema
        table_name - Name of database table
    """
    init_logger()

    db_params = DBConnectionParam(
        DB_USER, DB_USER_PW, DB_HOST, DB_PORT, DB_NAME
    )

    try:
        logger.info("filepath %s, Schema %s", filepath, schema_name)
        _save_file_to_db(
            filepath,
            schema_name,
            table_name,
            db_params,
            batch_size=batch_size,
            dispose_at_end=True,
            upsert_rows=upsert_rows,
        )
    except Exception as e:
        logger.exception("Failed to save file to DB")
        raise e
