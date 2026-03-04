"""
GraphQL methods to CRUD organization in Dewrangle
"""

import os
import logging
from pprint import pformat

from d3b_api_client_cli.config import DEWRANGLE_MAX_PAGE_SIZE
from d3b_api_client_cli.dewrangle.graphql.common import (
    exec_query,
)
from d3b_api_client_cli.dewrangle.graphql.organization import (
    queries,
    mutations,
)
from d3b_api_client_cli.config import DEWRANGLE_DIR
from d3b_api_client_cli.utils import write_json

logger = logging.getLogger(__name__)


def upsert_organization(variables):
    """
    Upsert organization in Dewrangle

    :param variables: Organization attributes (see Dewrangle graphql schema)
    :type variables: dict
    :rtype: dict
    :returns: the organization
    """
    params = {"input": variables}
    # Check if this is an update or create
    orgs = read_organizations(log_output=False)
    update = False
    for org in orgs:
        if org["name"] == variables["name"]:
            update = True
            break

    if update:
        key = "Update"
        params.update({"id": org["id"]})
        dwid = org["id"]
        resp = exec_query(mutations.update_organization, variables=params)
    else:
        key = "Create"
        resp = exec_query(mutations.create_organization, variables=params)

    errors = resp.get(f"organization{key}", {}).get("errors")
    if errors:
        logger.warning(f"‼️  {key} organization failed:\n{pformat(resp)}")
    else:
        logger.warning(f"✅ {key} organization succeeded:\n{pformat(resp)}")

    result = resp[f"organization{key}"]["organization"]

    return result


def delete_organization(dewrangle_org_id=None, dewrangle_org_name=None):
    """
    Delete organization in Dewrangle by node ID or name

    :param dewrangle_org_id: Dewrangle node ID of the organization
    :type dewrangle_org_id: str
    :param dewrangle_org_name: Dewrangle name of organization
    :type dewrangle_org_name: str
    :rtype: dict
    :returns: the response
    """
    if not (dewrangle_org_id or dewrangle_org_name):
        raise Exception(
            "You must provide either the dewrangle_org_id or dewrangle_org_name"
        )

    if dewrangle_org_name:
        node_id = read_organization(dewrangle_org_name=dewrangle_org_name).get(
            "id"
        )
    else:
        node_id = dewrangle_org_id

    resp = exec_query(mutations.delete_organization, variables={"id": node_id})

    errors = resp.get("organizationDelete", {}).get("errors")
    if errors:
        result = errors
        logger.warning(f"🚮 ‼️  Delete organization failed:\n{pformat(resp)}")
    else:
        logger.info(f"🚮 Deleted organization:\n{pformat(resp)}")
        result = resp["organizationDelete"]["organization"]
        result["id"] = node_id

    return result


def read_organizations(output_dir=DEWRANGLE_DIR, log_output=True):
    """
    Fetch organizations that the client has access to

    :rtype: dict
    :returns: the organizations
    """
    organizations = paginate_organizations()
    logger.info(f"Fetched {len(organizations)} organizations")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "Organization.json")
        write_json(organizations, filepath)
        logger.info(f"✏️  Wrote {len(organizations)} organization to {filepath}")

    if log_output:
        logger.info(f"👨‍👩‍👦 Organizations:\n{pformat(organizations)}")

    return organizations


def read_organization(dewrangle_org_name=None, dewrangle_org_id=None):
    """
    Fetch Dewrangle organization by name
    """
    if not (dewrangle_org_id or dewrangle_org_name):
        raise Exception(
            "You must provide either the dewrangle_org_id or dewrangle_org_name"
        )
    key = "id" if dewrangle_org_id else "name"
    value = dewrangle_org_id if dewrangle_org_id else dewrangle_org_name

    found_org = None
    orgs = read_organizations(log_output=False)
    for org in orgs:
        if org[key] == value:
            found_org = org
            break

    return found_org


def paginate_organizations(org_page_size=DEWRANGLE_MAX_PAGE_SIZE):
    """
    Fetch all organizations that the viewer has access to

    Use Relay graphql pagination
    """
    logger.info("📄 Paginating Dewrangle organizations ...")

    variables = {"first": org_page_size}
    resp = exec_query(queries.organization_users, variables=variables)

    organizations = []
    count = 0
    has_next_page = True
    while has_next_page:
        org_users = resp["viewer"]["organizationUsers"]["edges"]

        count += len(org_users)
        total = resp["viewer"]["organizationUsers"]["totalCount"]

        if not total:
            has_next_page = False
            continue

        logger.info(f"Collecting {count}/{total} organizations")
        for org_user in org_users:
            organizations.append(org_user["node"]["organization"])

        page_info = resp["viewer"]["organizationUsers"]["pageInfo"]

        has_next_page = page_info["hasNextPage"]
        end_cursor = page_info["endCursor"]

        if has_next_page and end_cursor:
            variables.update({"after": end_cursor})
            resp = exec_query(queries.organization_users, variables=variables)

    return organizations


def get_org_by_name(org_name):
    """
    Fetch organization from Dewrangle
    """
    orgs = paginate_organizations()

    found_org = {}
    for org in orgs:
        if org["name"] == org_name:
            found_org = org
            break
    return found_org
