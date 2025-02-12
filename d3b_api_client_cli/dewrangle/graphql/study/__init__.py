"""
GraphQL methods to CRUD study in Dewrangle
"""

import os
import logging
from pprint import pformat
from typing import Optional

import gql

from d3b_api_client_cli.dewrangle.graphql.common import (
    exec_query,
)
from d3b_api_client_cli.dewrangle.graphql.study import (
    queries,
    mutations,
)
from d3b_api_client_cli.dewrangle.graphql.organization import (
    paginate_organizations,
)
from d3b_api_client_cli.config import config
from d3b_api_client_cli.utils import (
    write_json,
    kf_id_to_global_id,
    global_id_to_kf_id,
)

logger = logging.getLogger(__name__)

DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
DEWRANGLE_MAX_PAGE_SIZE = config["dewrangle"]["pagination"]["max_page_size"]


def upsert_global_descriptors(
    study_file_id: str,
    skip_unavailable_descriptors: Optional[bool] = True
) -> dict:
    """
    Trigger the operation to upsert global descriptors in Dewrangle

    Args:
    - skip_unavailable_descriptors: If true any errors due to a descriptor
    """
    logger.info(
        "🛸 Upsert global descriptors for study file: %s", study_file_id
    )
    variables = {
        "input": {
            "studyFileId": study_file_id,
            "skipUnavailableDescriptors": skip_unavailable_descriptors,
        }
    }
    resp = exec_query(mutations.upsert_global_descriptors, variables=variables)

    key = "globalDescriptorUpsert"
    mutation_errors = resp.get(key, {}).get("errors")
    job_errors = resp.get(key, {}).get(
        "job", {}).get("errors", {}).get("edges", [])

    if mutation_errors or job_errors:
        logger.error("❌ %s for study failed", key)
        if mutation_errors:
            logger.error("❌ Mutation Errors:\n%s", pformat(mutation_errors))
        if job_errors:
            logger.error("❌ Job Errors:\n%s", pformat(job_errors))
    else:
        logger.info("✅ %s for study succeeded:\n%s", key, pformat(resp))

    return resp


def upsert_study(
    variables: dict, organization_id: str, study_id: str = None
) -> dict:
    """
    Upsert study in Dewrangle

    Arguments:
        variables - Study attributes (see Dewrangle graphql schema)
        organization_id - Dewrangle ID of organization
        study_id - Kids First study ID or Dewrangle global ID

    Returns:
        Dewrangle study dict
    """
    update = False
    global_id = None
    if study_id and study_id.startswith("SD_"):
        global_id = kf_id_to_global_id(study_id)

    if not global_id:
        global_id = variables.get("globalId", "")

    study = None
    if global_id:
        study = find_study(global_id)

    if study:
        update = True
        if study["organization_id"] != organization_id:
            raise ValueError(
                "❌ This study is already part of another organization:"
                f" {study['organization_id']}. You cannot change its"
                " organization"
            )

    params = {"input": variables}

    if update:
        key = "Update"
        params.update({"id": study["id"]})
        dwid = study["id"]
        resp = exec_query(mutations.update_study, variables=params)
    else:
        key = "Create"
        params["input"].update({"organizationId": organization_id})
        params.pop("id", None)
        resp = exec_query(mutations.create_study, variables=params)
        dwid = resp["studyCreate"]["study"]["id"]

    errors = resp.get(f"study{key}", {}).get("errors")
    if errors:
        logger.error("❌ %s study failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s study succeeded:\n%s", key, pformat(resp))

    result = resp[f"study{key}"]["study"]
    result["id"] = dwid
    result["organization_id"] = organization_id

    return result


def delete_study(
    _id: str,
    delete_safety_check: bool = True,
) -> dict:
    """
    Delete study in Dewrangle

    Arguments:
        _id - either a Kids First formatted ID or Dewrangle global ID
        delete_safety_check - only delete if this is False

    Returns:
        Response from Dewrangle
    """
    node_id = _id
    if _id.startswith("SD_"):
        study = find_study(kf_id_to_global_id(_id))
        node_id = study.get("id")
        if not node_id:
            logger.warning(
                "⚠️  Could not find associated dewrangle ID."
                " Delete study %s ABORTED",
                _id,
            )
            return

    resp = exec_query(
        mutations.delete_study,
        variables={"id": node_id},
        delete_safety_check=delete_safety_check,
    )

    errors = resp.get("studyDelete", {}).get("errors")
    key = "Delete"
    if errors:
        result = errors
        logger.error("❌ %s study failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s study succeeded:\n%s", key, pformat(resp))
        result = resp["studyDelete"]["study"]
        result["id"] = node_id

    return result


def read_studies(
    output_dir: str = DEWRANGLE_DIR, log_output: bool = True
) -> list[dict]:
    """
    Fetch studies that the client has access to

    Arguments:
        output_dir - directory where study metadata will be written
        log_output - whether to log study dicts

    Returns:
        List of study dicts
    """
    data = paginate_studies()

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "Study.json")
        write_json(data, filepath)
        logger.info("✏️  Wrote %s study to %s", len(data), filepath)

    if log_output:
        logger.info("🔬 Studies:\n%s", pformat(data))

    return data


def read_study(node_id: str) -> dict:
    """
    Fetch study by node id
    """
    variables = {"id": node_id}
    resp = exec_query(queries.study, variables=variables)
    study = resp.get("node", {})

    if study:
        logger.info(
            "🔎  Found Dewrangle study %s:\n%s",
            study["globalId"],
            pformat(study),
        )
    else:
        logger.error("❌ Not Found: dewrangle study %s", node_id)

    return study


def paginate_studies(
    organizations=None, study_page_size=DEWRANGLE_MAX_PAGE_SIZE
):
    """
    Fetch all studies in all organizations that the viewer has access to

    Use Relay graphql pagination
    """
    if not organizations:
        organizations = paginate_organizations()

    logger.info("📄 Paginating Dewrangle studies ...")

    studies = {}
    for org in organizations:
        variables = {"first": study_page_size, "id": org["id"]}
        resp = exec_query(queries.org_studies, variables=variables)

        count = 0
        has_next_page = True
        while has_next_page:
            org_studies = resp["node"]["studies"]["edges"]
            page_info = resp["node"]["studies"]["pageInfo"]

            count += len(org_studies)
            total = resp["node"]["studies"]["totalCount"]

            if not total:
                has_next_page = False
                continue

            logger.info("******* Organization %s *******", org["name"])
            logger.info(
                "Collecting %s/%s studies for org %s", count, total, org["name"]
            )
            # Add studies to ouput
            for s in org_studies:
                study = s["node"]
                study["organization_id"] = org["id"]
                kf_id = global_id_to_kf_id(study["globalId"])
                study["kf_id"] = kf_id
                studies[study["globalId"]] = study
                logger.info("Found study %s", study["globalId"])

            # Fetch next page if there is one
            has_next_page = page_info["hasNextPage"]
            end_cursor = page_info["endCursor"]
            if has_next_page and end_cursor:
                variables.update({"after": end_cursor})
                resp = exec_query(queries.org_studies, variables=variables)

    return studies


def find_study(study_global_id: str) -> dict:
    """
    Find study using Dewrangle global ID. Use this when you don't know the
    org ID
    """
    studies = paginate_studies()
    return studies.get(study_global_id, {})


def get_study_by_id(study_id: str, org_node_id: str) -> dict:
    """
    Fetch study from Dewrangle by KF ID or global ID
    """
    if study_id.startswith("SD_"):
        study_id = kf_id_to_global_id(study_id)

    resp = exec_query(
        queries.study_by_global_id,
        variables={"id": org_node_id, "filter": {"query": study_id}},
    )

    errors = resp.get("studyQuery", {}).get("errors")
    key = "Get"
    if errors:
        result = errors
        logger.error("❌ %s study failed:\n%s", key, pformat(resp))
    else:
        result = {}
        logger.info("✅ %s study succeeded:\n%s", key, pformat(resp))
        edges = resp["studyQuery"]["node"]["studies"]["edges"]
        if not edges:
            logger.error("❌ Study %s not found!", study_id)
        else:
            result = edges[0]["node"]

    return result
