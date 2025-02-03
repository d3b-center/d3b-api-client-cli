"""
GraphQL methods to created and delete AWS credential in Dewrangle
"""

import os
import logging
from pprint import pformat, pprint
from collections import defaultdict

import gql

from d3b_api_client_cli.dewrangle.graphql.common import (
    exec_query,
)
from d3b_api_client_cli.dewrangle.graphql.study import (
    paginate_studies,
    find_study,
)
from d3b_api_client_cli.dewrangle.graphql.credential import (
    queries,
    mutations,
)
from d3b_api_client_cli.config import config
from d3b_api_client_cli.utils import (
    write_json,
)

logger = logging.getLogger(__name__)

DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
DEWRANGLE_MAX_PAGE_SIZE = config["dewrangle"]["pagination"]["max_page_size"]


def upsert_credential(
    variables: dict, study_id=None, study_global_id=None
) -> dict:
    """
    Upsert credential in Dewrangle

    Arguments:
        variables - Credential attributes (see Dewrangle graphql schema)
        study_id - Graphql node ID of the credential's study
        study_global_id - Global ID of credential's study
    Returns:
        Dewrangle credential dict
    """
    if not (study_id or study_global_id):
        raise ValueError(
            "❌ Either the graphql node ID or global ID of the credential's"
            " study must be provided to either create or update the credential"
        )

    credential_key = variables.get("key")

    # If no study id provided, try querying for it via global ID
    if not study_id:
        study_id = find_study(study_global_id).get("id")

    # Try finding existing credential
    if not credential_key:
        credential = None
    else:
        credential = find_credential(credential_key, study_id)

    params = {"input": variables}

    if credential:
        key = "Update"

        # Remove any immutable fields
        for immutable_attr in ["key", "secret", "type", "studyId"]:
            params["input"].pop(immutable_attr, None)

        params.update({"id": credential["id"]})
        resp = exec_query(mutations.update_credential, variables=params)
    else:
        key = "Create"
        params["input"].update({"studyId": study_id})
        params.pop("id", None)
        resp = exec_query(mutations.create_credential, variables=params)

    errors = resp.get(f"credential{key}", {}).get("errors")
    if errors:
        logger.error("❌ %s credential failed:\n%s", key, pformat(resp))
        result = errors
    else:
        logger.info("✅ %s credential succeeded:\n%s", key, pformat(resp))

        result = resp[f"credential{key}"]["credential"]
        result["id"] = result["id"]
        result["study_id"] = result["study"]["id"]

    return result


def delete_credential(
    node_id: str = None,
    credential_key: str = None,
    study_global_id: str = None,
    delete_safety_check: bool = True,
) -> dict:
    """
    Delete credential in Dewrangle

    Arguments:
        node_id - Dewrangle graphql node ID
        credential_key - Credential key
        study_global_id - Global ID of credential's study
        delete_safety_check - only delete if this is False

    Returns:
        Response from Dewrangle
    """
    if not (node_id or (credential_key and study_global_id)):
        raise ValueError(
            "❌ You must provide either the credential graphql ID or"
            " credential key and study global ID to look up the credential"
        )
    if credential_key:
        study_id = find_study(study_global_id).get("id")
        credential = find_credential(credential_key, study_id)
        node_id = credential.get("id")
        if not node_id:
            logger.warning(
                "⚠️  Could not find associated dewrangle ID."
                " Delete credential %s ABORTED",
                node_id,
            )
            return

    resp = exec_query(
        mutations.delete_credential,
        variables={"id": node_id},
        delete_safety_check=delete_safety_check,
    )

    errors = resp.get("credentialDelete", {}).get("errors")
    key = "Delete"
    if errors:
        result = errors
        logger.error("❌ %s credential failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s credential succeeded:\n%s", key, pformat(resp))
        result = resp["credentialDelete"]["credential"]
        result["id"] = node_id

    return result


def read_credential(node_id: str) -> dict:
    """
    Fetch credential by node id
    """
    variables = {"id": node_id}
    resp = exec_query(queries.credential, variables=variables)
    credential = resp.get("node", {})

    if credential:
        logger.info(
            "🔎  Found Dewrangle credential %s:\n%s",
            credential["globalId"],
            pformat(credential),
        )
    else:
        logger.error("❌ Not Found: dewrangle credential %s", node_id)

    return credential


def read_credentials(
    study_global_id=None,
    output_dir: str = DEWRANGLE_DIR,
    log_output: bool = True,
) -> list[dict]:
    """
    Fetch credentials that the client has access to

    Arguments:
        study_global_id - Global ID of credential's study
        output_dir - directory where study metadata will be written
        log_output - whether to log study dicts

    Returns:
        List of credential dicts
    """
    study_id = None
    if study_global_id:
        study_id = find_study(study_global_id).get("id")

    data = paginate_credentials(study_id=study_id)

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "Credential.json")
        write_json(data, filepath)
        logger.info("✏️  Wrote %s credential to %s", len(data), filepath)

    if log_output:
        logger.info("🔐 Credentials:\n%s", pformat(data))

    return data


def paginate_credentials(
    studies=None, study_id=None, dewrangle_page_size=DEWRANGLE_MAX_PAGE_SIZE
) -> dict:
    """
    Fetch all credentials in all studies that the viewer has access to

    Optionally filter credentials by study. Uses Relay graphql pagination

    Returns:
        dict that looks like this
        {
           "credential_key1": {
                "study1": {
                    <credential payload>
                },
                "study2": {
                    <credential payload>
                }
           },
           "credential_key2": ...
        }
    """
    if not studies:
        studies = paginate_studies()

    logger.info("📄 Paginating Dewrangle studies ...")

    credentials = defaultdict(lambda: defaultdict(dict))
    for study in studies.values():
        if study_id and (study.get("id") != study_id):
            continue
        variables = {"first": dewrangle_page_size, "id": study["id"]}
        resp = exec_query(queries.study_credentials, variables=variables)

        count = 0
        has_next_page = True
        while has_next_page:
            study_credentials = resp["node"]["credentials"]["edges"]
            page_info = resp["node"]["credentials"]["pageInfo"]

            count += len(study_credentials)
            total = resp["node"]["credentials"]["totalCount"]

            if not total:
                has_next_page = False
                continue

            logger.info(
                "Collecting %s credentials for study %s",
                count / total,
                study["name"],
            )
            # Add credentials to ouput
            for s in study_credentials:
                credential = s["node"]
                credential["study_id"] = study["id"]
                credential["study_global_id"] = study["globalId"]
                credentials[credential["key"]][study["id"]] = credential

            # Fetch next page if there is one
            has_next_page = page_info["hasNextPage"]
            end_cursor = page_info["endCursor"]
            if has_next_page and end_cursor:
                variables.update({"after": end_cursor})
                resp = exec_query(
                    queries.study_credentials, variables=variables
                )

    return dict(credentials)


def find_credential(credential_key: str, study_id: str) -> dict:
    """
    Find credential using credential key and study id.
    """
    credentials = paginate_credentials(study_id=study_id)
    return credentials.get(credential_key, {}).get(study_id, {})
