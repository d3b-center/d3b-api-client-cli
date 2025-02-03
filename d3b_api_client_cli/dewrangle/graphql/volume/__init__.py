"""
GraphQL methods to crud Volumes in Dewrangle
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
from d3b_api_client_cli.dewrangle.graphql.volume import (
    queries,
    mutations,
)
from d3b_api_client_cli.dewrangle.graphql.credential import find_credential
from d3b_api_client_cli.dewrangle.graphql.job import poll_job
from d3b_api_client_cli.config import config
from d3b_api_client_cli.utils import (
    write_json,
)

logger = logging.getLogger(__name__)

DEWRANGLE_DIR = config["dewrangle"]["output_dir"]
DEWRANGLE_MAX_PAGE_SIZE = config["dewrangle"]["pagination"]["max_page_size"]
DELIMITER = "::"
# Wait 30s between querying Dewrangle
POLL_LIST_AND_HASH_INTERVAL_SECS = 30


def upsert_volume(
    variables: dict, study_id=None, study_global_id=None, credential_key=None
) -> dict:
    """
    Upsert volume in Dewrangle

    Arguments:
        variables - Volume attributes (see Dewrangle graphql schema)
        study_id - Graphql node ID of the credential's study
        study_global_id - Global ID of credential's study
    Returns:
        Dewrangle volume dict
    """
    if not (study_id or study_global_id):
        raise ValueError(
            "❌ Either the graphql node ID or global ID of the volume's"
            " study must be provided to either create or update the volume"
        )

    # If no study id provided, try querying for it via global ID
    if not study_id:
        study_id = find_study(study_global_id).get("id")

    # If not credential provided, try querying for it
    credential_id = variables.get("credentialId")
    if not credential_id:
        credential_id = find_credential(credential_key, study_id)["id"]
        variables["credentialId"] = credential_id

    # Try finding existing volume
    bucket = variables.get("name")
    path_prefix = variables.get("pathPrefix")
    if not (bucket and path_prefix):
        volume = None
    else:
        volume = find_volume(bucket, path_prefix, study_id)

    params = {"input": variables}

    if volume:
        key = "Update"

        params["input"] = {"credentialId": credential_id}
        params.update({"id": volume["id"]})
        resp = exec_query(mutations.update_volume, variables=params)
    else:
        key = "Create"
        params["input"].update({"studyId": study_id})
        params.pop("id", None)
        resp = exec_query(mutations.create_volume, variables=params)

    errors = resp.get(f"volume{key}", {}).get("errors")
    entity = "volume"
    if errors:
        logger.error("❌ %s %s failed:\n%s", key, entity, pformat(resp))
        result = errors
    else:
        logger.info("✅ %s %s succeeded:\n%s", key, entity, pformat(resp))

        result = resp[f"{entity}{key}"][entity]
        result["id"] = result["id"]
        result["study_id"] = result["study"]["id"]

    return result


def delete_volume(
    node_id: str = None,
    bucket: str = None,
    path_prefix: str = None,
    study_global_id: str = None,
    delete_safety_check: bool = True,
) -> dict:
    """
    Delete volume in Dewrangle

    Arguments:
        node_id - Dewrangle graphql node ID
        bucket - S3 bucket name
        path_prefix - Path in the S3 bucket
        study_global_id - Global ID of credential's study
        delete_safety_check - only delete if this is False

    Returns:
        Response from Dewrangle
    """
    if not (node_id or (bucket and study_global_id)):
        raise ValueError(
            "❌ You must provide either the volume graphql ID or"
            " volume key and study global ID to look up the volume"
        )
    if bucket:
        study_id = find_study(study_global_id).get("id")
        volume = find_volume(bucket, path_prefix, study_id)
        node_id = volume.get("id")
        if not node_id:
            logger.warning(
                "⚠️  Could not find associated dewrangle ID."
                " Delete volume %s ABORTED",
                node_id,
            )
            return

    resp = exec_query(
        mutations.delete_volume,
        variables={"id": node_id},
        delete_safety_check=delete_safety_check,
    )

    errors = resp.get("volumeDelete", {}).get("errors")
    key = "Delete"
    if errors:
        result = errors
        logger.error("❌ %s volume failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s volume succeeded:\n%s", key, pformat(resp))
        result = resp["volumeDelete"]["volume"]
        result["id"] = node_id

    return result


def read_volume(node_id: str) -> dict:
    """
    Fetch volume by node id
    """
    variables = {"id": node_id}
    resp = exec_query(queries.volume, variables=variables)
    volume = resp.get("node", {})

    if volume:
        logger.info(
            "🔎  Found Dewrangle volume %s:\n%s",
            volume["globalId"],
            pformat(volume),
        )
    else:
        logger.error("❌ Not Found: dewrangle volume %s", node_id)

    return volume


def read_volumes(
    study_global_id=None,
    output_dir: str = DEWRANGLE_DIR,
    log_output: bool = True,
) -> list[dict]:
    """
    Fetch volumes that the client has access to

    Arguments:
        study_global_id - Global ID of volume's study
        output_dir - directory where study metadata will be written
        log_output - whether to log study dicts

    Returns:
        List of volume dicts
    """
    study_id = None
    if study_global_id:
        study_id = find_study(study_global_id).get("id")

    data = paginate_volumes(study_id=study_id)

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "Volume.json")
        write_json(data, filepath)
        logger.info("✏️  Wrote %s volume to %s", len(data), filepath)

    if log_output:
        logger.info("🔐 Volumes:\n%s", pformat(data))

    return data


def _volume_key(bucket: str, path_prefix: str) -> str:
    """
    Helper method to create a dict key for the output of paginate_volumes

    This key uniquely identifies a volume among volumes within a study
    """
    if path_prefix is None:
        path_prefix = ""
    return f"{bucket}{DELIMITER}{path_prefix}"


def _store_volumes(
    study_volumes: list[dict], volumes: dict, study: dict
) -> dict:
    """
    Storage page of volumes into volumes dict
    """
    for s in study_volumes:
        volume = s["node"]
        volume["study_id"] = study["id"]
        volume["study_global_id"] = study["globalId"]

        bucket = volume["name"]
        path_prefix = volume["pathPrefix"]
        volumes[_volume_key(bucket, path_prefix)][study["id"]] = volume

    return volumes


def paginate_volumes(
    studies=None, study_id=None, dewrangle_page_size=DEWRANGLE_MAX_PAGE_SIZE
) -> dict:
    """
    Fetch all volumes in all studies that the viewer has access to

    Optionally filter volumes by study. Uses Relay graphql pagination

    Returns:
        dict that looks like this
        {
            "bucket1::/path/to/myfile": {
                "study1": {
                    <volume payload>
                },
                "study2": {
                    <volume payload>
                }
           },
           "bucket1::/path/to/another": ...,
           "bucket2::": ...
        }
    """
    if not studies:
        studies = paginate_studies()

    logger.info("📄 Paginating Dewrangle volumes ...")

    volumes = defaultdict(lambda: defaultdict(dict))
    for study in studies.values():
        if study_id and (study.get("id") != study_id):
            continue
        variables = {"first": dewrangle_page_size, "id": study["id"]}
        resp = exec_query(queries.study_volumes, variables=variables)

        count = 0
        has_next_page = True
        while has_next_page:
            study_volumes = resp["node"]["volumes"]["edges"]
            page_info = resp["node"]["volumes"]["pageInfo"]

            count += len(study_volumes)
            total = resp["node"]["volumes"]["totalCount"]

            if not total:
                has_next_page = False
                continue

            logger.info(
                "Collecting %s volumes for study %s",
                count / total,
                study["name"],
            )
            # Add volumes in output
            volumes = _store_volumes(study_volumes, volumes, study)

            # Fetch next page if there is one
            has_next_page = page_info["hasNextPage"]
            end_cursor = page_info["endCursor"]
            if has_next_page and end_cursor:
                variables.update({"after": end_cursor})
                resp = exec_query(queries.study_volumes, variables=variables)

    return dict(volumes)


def find_volume(bucket: str, path_prefix: str, study_id: str) -> dict:
    """
    Find volume using S3 bucket name, path prefix, and study id.
    """
    volumes = paginate_volumes(study_id=study_id)
    return volumes.get(_volume_key(bucket, path_prefix), {}).get(study_id, {})


def list_and_hash(
    billing_group_id: str,
    volume_id: str = None,
    bucket: str = None,
    path_prefix: str = None,
    study_global_id: str = None,
) -> dict:
    """
    Trigger a list and hash volume job in Dewrangle

    Arguments:
        billing_group_id - Dewrangle graphql ID of billing group
        volume_id - Dewrangle graphql ID of volume
        bucket - S3 bucket name
        path_prefix - Path in the S3 bucket
        study_global_id - Global ID of volume's study

    Returns:
        Job from Dewrangle
    """
    key = "ListAndHash"

    if not billing_group_id:
        raise ValueError(
            "❌ Billing group ID is missing and required to hash a volume!"
        )

    if not (volume_id or (bucket and study_global_id)):
        raise ValueError(
            "❌ You must provide either the volume graphql ID or"
            " volume name and study global ID to look up the volume"
        )
    # Try querying for volume
    if not volume_id:
        study_id = find_study(study_global_id).get("id")
        if not study_id:
            raise ValueError(
                "❌ Could not find associated dewrangle ID for "
                f" study with ID {study_global_id}."
            )

        volume_id = find_volume(bucket, path_prefix, study_id).get("id")

    if not volume_id:
        raise ValueError(
            "❌ Could not find associated dewrangle ID for "
            f" {key} volume with ID {volume_id}."
        )

    variables = {
        "input": {
            "billingGroupId": billing_group_id,
        },
        "id": volume_id,
    }
    resp = exec_query(mutations.list_and_hash, variables=variables)

    errors = resp.get(f"volume{key}", {}).get("errors")
    if errors:
        result = errors
        logger.error("❌ %s volume failed:\n%s", key, pformat(resp))
    else:
        logger.info("✅ %s volume succeeded:\n%s", key, pformat(resp))
        result = resp[f"volume{key}"]["job"]

    return result


def hash_and_wait(
    billing_group_id: str,
    volume_id: str,
    bucket: str = None,
    path_prefix: str = None,
    study_global_id: str = None,
):
    """
    Trigger a list and hash volume job and poll for job status until the
    job is complete or fails

    Arguments:
        billing_group_id - Dewrangle graphql ID of billing group
        volume_id - Dewrangle graphql ID of volume
        bucket - S3 bucket name
        path_prefix - Path in the S3 bucket
        study_global_id - Global ID of volume's study
    """
    # List and hash volume
    job = list_and_hash(
        volume_id=volume_id,
        billing_group_id=billing_group_id,
        bucket=bucket,
        path_prefix=path_prefix,
        study_global_id=study_global_id,
    )
    return poll_job(
        job["id"], interval_seconds=POLL_LIST_AND_HASH_INTERVAL_SECS
    )
