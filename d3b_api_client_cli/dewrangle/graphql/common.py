"""
Common functions needed to execute GraphQL queries and mutations
"""

import logging

from gql import Client
from graphql import print_ast
from gql.transport.aiohttp import AIOHTTPTransport

from d3b_api_client_cli.config import (
    config,
    DEWRANGLE_DEV_PAT,
    DEWRANGLE_MAX_PAGE_SIZE,
)
from d3b_api_client_cli.dewrangle.graphql.queries import (
    viewer_query,
)
from d3b_api_client_cli import utils

logger = logging.getLogger(__name__)
graphql_client = None

gql_logger = logging.getLogger("gql.transport.aiohttp")
gql_logger.setLevel(level=logging.CRITICAL)


def create_graphql_client():
    """
    Create a gql GraphQL client that will exec queries asynchronously
    """
    base_url = config["dewrangle"]["base_url"]
    endpoint = config["dewrangle"]["endpoints"]["graphql"]
    base = base_url.rstrip("/")
    path = endpoint.lstrip("/")
    url = f"{base}/{path}"
    logger.info(f"🛠️  Setting up GraphQL client for {url}")

    if not DEWRANGLE_DEV_PAT:
        raise Exception(
            "❌ Cannot continue GraphQL operation because the environment"
            " variable, DEWRANGLE_DEV_PAT, is not set. Please create a"
            " personal access token on Dewrangle and set it in your environment"
            " in the DEWRANGLE_DEV_PAT variable"
        )
    headers = {"x-api-key": DEWRANGLE_DEV_PAT}

    transport = AIOHTTPTransport(url=url, headers=headers)

    # Create a GraphQL client using the defined transport
    return Client(
        transport=transport,
        fetch_schema_from_transport=False,
        execute_timeout=30,
    )


def exec_query(gql_query, variables=None):
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
    if "delete" in str_query.lower():
        utils.delete_safety_check(base_url)

    global graphql_client
    if not graphql_client:
        graphql_client = create_graphql_client()

    return graphql_client.execute(gql_query, variable_values=variables)


def viewer_entities():
    """
    Execute the viewer query, extract entities and flatten into lists of
    entities. Return a dict with lists of flattened entities
    """
    # NOTE: We should really implement pagination here, but for now we're
    # just going to try and pull all the studies in one query
    variables = {"first": DEWRANGLE_MAX_PAGE_SIZE}
    resp = exec_query(viewer_query, variables=variables)
    org_nodes = resp["viewer"]["organizationUsers"]["edges"]
    if org_nodes:
        organizations = [
            edge["node"]["organization"]
            for edge in resp["viewer"]["organizationUsers"]["edges"]
        ]
    else:
        organizations = []

    studies = []
    fhir_servers = []

    # orgs
    for org in organizations:
        # studies
        for edge in org["studies"]["edges"]:
            study = edge["node"]
            study["organization_id"] = org["id"]
            # fhir servers
            server = study["fhirServerDeployments"]
            ids = {
                "study_fhir_server_id": server["id"],
                "fhir_server_id": server["fhirServer"]["id"],
                "study_id": study["id"],
                "study_global_id": study["globalId"],
                "organization_id": org["id"],
            }
            server.update(ids)
            fhir_servers.append(server)
            studies.append(study)

    return {
        "organizations": organizations,
        "studies": studies,
        "study_fhir_servers": fhir_servers,
    }
