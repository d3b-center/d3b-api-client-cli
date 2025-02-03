"""
GraphQL methods to CRUD billing_group in Dewrangle
"""

import os
import logging
from pprint import pformat, pprint

import gql

from d3b_api_client_cli.dewrangle.graphql.common import (
    exec_query,
)
from d3b_api_client_cli.dewrangle.graphql.billing_group import (
    queries,
    mutations,
)
from d3b_api_client_cli.dewrangle.graphql.organization import (
    paginate_organizations,
)
from d3b_api_client_cli.config import config
from d3b_api_client_cli.utils import (
    write_json,
)

logger = logging.getLogger(__name__)

DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
DEWRANGLE_MAX_PAGE_SIZE = config["dewrangle"]["pagination"]["max_page_size"]


def create_or_find_billing_group(
    organization_id: str, cavatica_billing_group_id: str
) -> dict:
    """
    Create billing_group if it does not exist, otherwise return
    the existing billing group in Dewrangle

    Arguments:
        organization_id - Dewrangle ID of organization
        cavatica_billing_group_id - Cavatica billing group ID

    Returns:
        Dewrangle billing_group dict
    """
    billing_group = create_billing_group(
        organization_id, cavatica_billing_group_id
    )
    if not billing_group:
        return find_billing_group(cavatica_billing_group_id)
    else:
        return billing_group


def create_billing_group(
    organization_id: str, cavatica_billing_group_id: str
) -> dict:
    """
    Create billing_group in Dewrangle

    Arguments:
        organization_id - Dewrangle ID of organization
        cavatica_billing_group_id - Cavatica billing group ID

    Returns:
        Dewrangle billing_group dict
    """
    params = {
        "input": {
            "organizationId": organization_id,
            "cavaticaBillingGroupId": cavatica_billing_group_id,
        }
    }

    key = "Create"
    resp = exec_query(mutations.create_billing_group, variables=params)

    errors = resp.get(f"billingGroup{key}", {}).get("errors")
    if errors:
        logger.error("❌ %s billing_group failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s billing_group succeeded:\n%s", key, pformat(resp))

    result = resp["billingGroupCreate"]["billingGroup"]
    if not errors:
        result["organization_id"] = organization_id

    return result


def delete_billing_group(
    _id: str,
    delete_safety_check: bool = True,
) -> dict:
    """
    Delete billing_group in Dewrangle

    Arguments:
        _id -  Dewrangle ID
        delete_safety_check - only delete if this is False

    Returns:
        Response from Dewrangle
    """
    node_id = _id

    resp = exec_query(
        mutations.delete_billing_group,
        variables={"id": node_id},
        delete_safety_check=delete_safety_check,
    )

    errors = resp.get("billingGroupDelete", {}).get("errors")
    key = "Delete"
    if errors:
        result = errors
        logger.error("❌ %s billing_group failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s billing_group succeeded:\n%s", key, pformat(resp))
        result = resp["billingGroupDelete"]["billingGroup"]
        result["id"] = node_id

    return result


def read_billing_groups(
    output_dir: str = DEWRANGLE_DIR, log_output: bool = True
) -> list[dict]:
    """
    Fetch billing_groups that the client has access to

    Arguments:
        output_dir - directory where billing_group metadata will be written
        log_output - whether to log billing_group dicts

    Returns:
        List of billing_group dicts
    """
    data = paginate_billing_groups()

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "BillingGroup.json")
        write_json(data, filepath)
        logger.info("✏️  Wrote %s billing_group to %s", len(data), filepath)

    if log_output:
        logger.info("💰 BillingGroups:\n%s", pformat(data))

    return data


def read_billing_group(node_id: str) -> dict:
    """
    Fetch billing_group by node id
    """
    variables = {"id": node_id}
    resp = exec_query(queries.billing_group, variables=variables)
    billing_group = resp.get("node", {})

    if billing_group:
        logger.info(
            "🔎  Found Dewrangle billing_group %s:\n%s",
            billing_group["id"],
            pformat(billing_group),
        )
    else:
        logger.error("❌ Not Found: dewrangle billing_group %s", node_id)

    return billing_group


def paginate_billing_groups(
    organizations=None, billing_group_page_size=DEWRANGLE_MAX_PAGE_SIZE
):
    """
    Fetch all billing_groups in all organizations that the viewer has access to

    Use Relay graphql pagination
    """
    if not organizations:
        organizations = paginate_organizations()

    logger.info("📄 Paginating Dewrangle billing_groups ...")

    billing_groups = {}
    for org in organizations:
        variables = {"first": billing_group_page_size, "id": org["id"]}
        resp = exec_query(queries.org_billing_groups, variables=variables)

        count = 0
        has_next_page = True
        while has_next_page:
            org_billing_groups = resp["node"]["billingGroups"]["edges"]
            page_info = resp["node"]["billingGroups"]["pageInfo"]

            count += len(org_billing_groups)
            total = resp["node"]["billingGroups"]["totalCount"]

            if not total:
                has_next_page = False
                continue

            logger.info(
                "Collecting %s billing_groups for org %s",
                count / total,
                org["name"],
            )
            # Add billing_groups to ouput
            for s in org_billing_groups:
                billing_group = s["node"]
                billing_group["organization_id"] = org["id"]
                billing_groups[billing_group["cavaticaBillingGroupId"]] = (
                    billing_group
                )
                logger.info(
                    "Found billing_group %s",
                    billing_group["cavaticaBillingGroupId"],
                )

            # Fetch next page if there is one
            has_next_page = page_info["hasNextPage"]
            end_cursor = page_info["endCursor"]
            if has_next_page and end_cursor:
                variables.update({"after": end_cursor})
                resp = exec_query(
                    queries.org_billing_groups, variables=variables
                )

    return billing_groups


def find_billing_group(cavatica_billing_group_id: str) -> dict:
    """
    Find billing_group using cavatica billing group id.
    Use this when you don't know the org ID
    """
    billing_groups = paginate_billing_groups()

    return billing_groups.get(cavatica_billing_group_id, {})
