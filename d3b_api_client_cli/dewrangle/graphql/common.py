"""
Common functions needed to execute GraphQL queries and mutations
"""

import logging

from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from graphql import print_ast

from d3b_api_client_cli.config import (
    config,
    DEWRANGLE_DEV_PAT,
    check_dewrangle_http_config,
)
from d3b_api_client_cli import utils

DEWRANGLE_BASE_URL = config["dewrangle"]["base_url"]
DEWRANGLE_MAX_PAGE_SIZE = config["dewrangle"]["pagination"]["max_page_size"]
EXECUTION_TIMEOUT = config["dewrangle"]["client"]["execution_timeout"]

logger = logging.getLogger(__name__)
graphql_client = None

gql_logger = logging.getLogger("gql.transport.aiohttp")
gql_logger.setLevel(level=logging.CRITICAL)


def create_graphql_client() -> Client:
    """
    Create a gql GraphQL client that will exec queries asynchronously
    """
    # Ensure env vars are set
    check_dewrangle_http_config()

    base_url = config["dewrangle"]["base_url"]
    endpoint = config["dewrangle"]["endpoints"]["graphql"]
    url = f"{base_url}/{endpoint}"
    logger.info(
        "🛠️  Setting up GraphQL client for %s",
        f"{base_url.rstrip('/')}/{endpoint}",
    )
    headers = {"x-api-key": DEWRANGLE_DEV_PAT}

    transport = AIOHTTPTransport(url=url, headers=headers)

    # Create a GraphQL client using the defined transport
    return Client(
        transport=transport,
        fetch_schema_from_transport=True,
        execute_timeout=EXECUTION_TIMEOUT,
    )


def exec_query(gql_query, variables=None, delete_safety_check=True):
    """
    Execute a graphql query and handle errors gracefully

    :param gql_query: gql formatted GraphQL query
    :type gql_query: graphql.language.ast.DocumentNode
    :param variables: GraphQL query variables
    :type variables: dict
    :rtype: dict
    :returns: the GraphQL query response
    """
    base_url = config["dewrangle"]["base_url"]
    str_query = print_ast(gql_query)
    if delete_safety_check and "delete" in str_query.lower():
        utils.delete_safety_check(base_url)

    global graphql_client
    if not graphql_client:
        graphql_client = create_graphql_client()

    return graphql_client.execute(gql_query, variable_values=variables)
