"""
GraphQL methods to CRUD fhir_server in Dewrangle
"""

import os
import logging
from pprint import pformat

from d3b_api_client_cli.dewrangle.graphql.common import (
    exec_query,
)
from d3b_api_client_cli.dewrangle.graphql.fhir_server import (
    queries,
    mutations,
)
from d3b_api_client_cli.config import DEWRANGLE_DIR
from d3b_api_client_cli.utils import write_json

logger = logging.getLogger(__name__)


def upsert_fhir_server(
    dewrangle_organization_id, variables, oidc_client_secret=None
):
    """
    Upsert fhir_server in Dewrangle

    :param variables: FhirServer attributes (see Dewrangle graphql schema)
    :type variables: dict
    :rtype: dict
    :returns: the fhir_server
    """
    variables.update({"organizationId": dewrangle_organization_id})
    if oidc_client_secret:
        auth_config = variables.get("authConfig", {})
        auth_config["clientSecret"] = oidc_client_secret
        variables.update({"authConfig": auth_config})

    params = {"input": variables}

    # Check if this is an update or create
    servers = read_fhir_servers(dewrangle_organization_id)
    update = False
    for server in servers:
        if server["name"] == variables["name"]:
            update = True
            break

    if update:
        key = "Update"
        params["input"].pop("organizationId", None)
        params["input"].pop("type", None)
        params.update({"id": server["id"]})
        dwid = server["id"]
        resp = exec_query(mutations.update_fhir_server, variables=params)
    else:
        key = "Create"
        resp = exec_query(mutations.create_fhir_server, variables=params)

    errors = resp.get(f"fhirServer{key}", {}).get("errors")
    if errors:
        logger.warning(f"‼️  {key} fhir_server failed:\n{pformat(resp)}")
    else:
        logger.info(f"✅ {key} fhir_server succeeded:\n{pformat(resp)}")

    result = resp[f"fhirServer{key}"]["fhirServer"]

    return result


def read_fhir_servers(dewrangle_organization_id, output_dir=DEWRANGLE_DIR):
    """
    Fetch FhirServers that the client has access to

    :rtype: dict
    :returns: the FHIR servers
    """
    params = {"id": dewrangle_organization_id}
    resp = exec_query(queries.organization_fhir_servers, variables=params)
    data = [
        edge.get("node", {}) for edge in resp["node"]["fhirServers"]["edges"]
    ]
    logger.info(f"Fetched {len(data)} fhir_servers")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "FhirServer.json")
        write_json(data, filepath)
        logger.info(f"✏️  Wrote {len(data)} fhir_server to {filepath}")

    logger.info(f"🔥 FhirServers:\n{pformat(data)}")

    return data


def delete_fhir_server(node_id):
    """
    Delete fhir_server in Dewrangle

    :param node_id: Dewrangle node ID of the fhir_server
    :type node_id: str
    :rtype: dict
    :returns: the response
    """
    resp = exec_query(mutations.delete_fhir_server, variables={"id": node_id})

    errors = resp.get("fhirServerDelete", {}).get("errors")
    if errors:
        result = errors
        logger.warning(f"🚮 ‼️  Delete fhir_server failed:\n{pformat(resp)}")
    else:
        logger.info(f"🚮 Deleted fhir_server:\n{pformat(resp)}")
        result = resp["fhirServerDelete"]["fhirServer"]
        result["id"] = node_id

    return result
