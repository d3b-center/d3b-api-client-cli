"""
Delete FHIR resources in the FHIR server efficiently
"""

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from pprint import pformat

import requests
from requests.auth import HTTPBasicAuth

from d3b_api_client_cli.config import (
    config,
    KidsFirstFhirEntity,
    ImagingFhirEntity,
    DELETE_DIR,
)
from d3b_api_client_cli import utils
from d3b_api_client_cli.fhir.get import get_all, get

logger = logging.getLogger(__name__)

config = config["fhir"]
kf_to_fhir_mapping = config["mapping"]
entity_delete_order = reversed(
    [et.value for et in KidsFirstFhirEntity]
    + [et.value for et in ImagingFhirEntity]
)

LOCAL_HOSTS = {
    "localhost",
    "127.0.0.1",
}


def _do_delete(base_url, endpoint, resource_id, username, password):
    """
    Helper function to delete FHIR resource
    """
    url = "/".join(
        part.strip("/") for part in [base_url, endpoint, resource_id]
    )
    headers = {"Content-Type": "application/json"}
    results = {
        "url": url,
        "resp": None,
        "failed": False,
    }
    try:
        resp = utils.send_request(
            "delete",
            url,
            ignore_status_codes={409},
            headers=headers,
            auth=HTTPBasicAuth(username, password),
        )
    except requests.exceptions.HTTPError as e:
        if "404 Client Error" in str(e):
            results["failed"] = True
            results["resp"] = {"id": resource_id}
        else:
            raise
    else:
        results["resp"] = resp.json()

    if resp.status_code == 409:
        logger.info(f"Conflict detected for {url}." " Delete in next round")
        issue = resp.json()["issue"]
        logger.info(f"Conflict Details: {pformat(issue)}")

    return results


def _process_result(result, results, i, total):
    url = result.get("url")
    url_path = urlparse(url).path
    resp = result.get("resp")
    failed = result["failed"]
    if not failed:
        results["success"].append(resp)
        logger.info(f"DELETE {url_path}, #{i + 1}/{total}")
    else:
        results["failed"].append(resp)
        logger.info(f"FAILED DELETE {url_path} #{i + 1}/{total}")

    return resp


def delete_entities(
    base_url, entities, username, password, use_async=True, safety_check=True
):
    """
    Delete the supplied FHIR entities in FHIR server

    Default behavior only deletes resources at localhost unless
    safety_check=False

    :param base_url: Base url of the FHIR service
    :type base_url: str
    :param entities: list of FHIR resources to delete
    :type entities: list of dict
    :param username: username of the API admin user
    :type username: str
    :param password: password of the API admin user
    :type password: str
    :param use_async: A flag to determine whether to use multi-threading when
    sending requests to the server
    :type use_async: boolean
    :param safety_check: A flag to prevent deleting in non-local servers
    :type safety_check: boolean
    :returns: None
    """
    if safety_check and (not utils.is_localhost(base_url)):
        raise Exception(
            f"❌ Cannot delete from {base_url} because safety_check is ENABLED. "
            f"Resources that are not in {LOCAL_HOSTS} will not be deleted "
            "unless you set safety_check=False."
        )
    logger.info(f"🚮 Deleting len(entities) from FHIR service: {base_url}")

    start_time = time.time()

    results = {"failed": [], "success": []}
    total = len(entities)
    if use_async:
        logger.info("⚡️ Using async deleting ...")
        with ThreadPoolExecutor() as tpex:
            futures = []
            for i, entity in enumerate(entities):
                resource_id = entity["id"]
                endpoint = entity["resourceType"]
                futures.append(
                    tpex.submit(
                        _do_delete,
                        base_url,
                        endpoint,
                        resource_id,
                        username,
                        password,
                    )
                )
            for i, f in enumerate(as_completed(futures)):
                result = f.result()
                _process_result(result, results, i, total)
    else:
        logger.info("🐌 Using synchronous deleting ...")
        for i, entity in enumerate(entities):
            resource_id = entity["id"]
            endpoint = entity["resourceType"]
            result = _do_delete(
                base_url, endpoint, resource_id, username, password
            )
            _process_result(result, results, i, total)

    logger.info(
        f"⏰ Elapsed time (hh:mm:ss): {utils.elapsed_time_hms(start_time)}"
    )

    return results


def _delete_specimens(
    base_url,
    output_dir,
    study_id,
    username,
    password,
    use_async,
    safety_check,
):
    """
    Delete FHIR specimens in FHIR server

    Specimens must be deleted in a different way than the other entities
    since their references to other specimens are not known ahead of time.

    This deletion works by attempting to delete specimens in whatever order
    they are fetched until there are no more specimens left in the server
    """
    entity_type = "specimen"
    params = {"_total": "accurate", "_tag": study_id}
    results = {"success": {}, "failed": {}}
    while True:
        # Get all specimens
        data = get_all(
            base_url,
            "Specimen",
            username,
            password,
            params=params,
        )
        if not data:
            logger.info("0️⃣  No more Specimen resources to delete")
            break

        logger.info(f"🚮 Try deleting Specimen {len(data)} resources")

        # Try deleting specimens
        r = delete_entities(
            base_url,
            data,
            username,
            password,
            use_async=use_async,
            safety_check=safety_check,
        )
        results["success"].update(r["success"])
        results["failed"].update(r["failed"])

    if results["failed"]:
        fp = os.path.join(output_dir, "failed", f"{entity_type}.json")
        utils.write_json(results["failed"], fp)
        logger.info(
            f"❌ Failed to delete {len(results['failed'])} {entity_type}"
        )

    if results["success"]:
        fp = os.path.join(output_dir, "success", f"{entity_type}.json")
        utils.write_json(results["success"], fp)
        logger.info(f"✅ Deleted {len(results['success'])} {entity_type}")

    return results


def delete_all_of_type(
    base_url,
    entity_type,
    output_dir,
    study_id=None,
    username=None,
    password=None,
    use_async=True,
    safety_check=True,
    use_kf_entity_tags=True,
):
    """
    Delete all FHIR resources in the server by type. If study_id is provided
    then delete only the resources which have been tagged by that study

    :param base_url: Base url of the FHIR service
    :type base_url: str
    :param entity_type: One of Kids First FHIR entity types in
    d3b_api_client_cli.config.KidsFirstFhirEntity
    :type entity_type: str
    :param output_dir: Where delete results will be written
    :type output_dir: str
    :param study_id: the study_id to filter resources to delete
    :type study_id: str
    :param username: username of the API admin user
    :type username: str
    :param password: password of the API admin user
    :type password: str
    :param use_async: A flag to determine whether to use multi-threading when
    sending requests to the server
    :type use_async: boolean
    :param safety_check: A flag to prevent deleting in non-local servers
    :type safety_check: boolean
    :param use_kf_entity_tags: A flag to include the KF entity type in the
    query params. This is used when deleting from the new FHIR servers
    :type use_kf_entity_tags: boolean

    :returns: None
    """
    if safety_check and (not utils.is_localhost(base_url)):
        raise Exception(
            f"❌ Cannot delete from {base_url} because safety_check is ENABLED. "
            f"Resources that are not in {LOCAL_HOSTS} will not be deleted "
            "unless you set safety_check=False."
        )

    if not (username and password):
        username = config["username"]
        password = config["password"]

    start_time = time.time()

    # Get the mapping of KF type to FHIR resource type and the required
    # query parameters to search for this entity in FHIR
    entity_config = kf_to_fhir_mapping.get(entity_type)
    if not entity_config:
        raise Exception(
            f"❌ Aborting delete. No configuration for {entity_type} in:\n"
            f"{pformat(kf_to_fhir_mapping)}"
        )

    # Setup query params
    tags = []
    if use_kf_entity_tags:
        # In new FHIR servers, we tag the resource with the KF entity type
        # and we MUST include this in the query parameters otherwise we
        # won't be able to differentiate between some entities of the
        # same resource type (e.g. drs doc ref vs drs doc ref index which
        # both have resource type DocumentReference)
        tags = [entity_type]

    if study_id:
        tags.append(study_id)

    params = {"_tag": tags}
    params.update(entity_config["params"])
    endpoint = entity_config["endpoint"]

    # Fetch all pages of data
    data = get_all(
        base_url,
        endpoint,
        username,
        password,
        params=params,
    )
    # Delete all entities of type
    results = {"failed": [], "success": []}
    if data:
        logger.info(f"🚮 Begin deleting {entity_type} {len(data)} resources")
        results = delete_entities(
            base_url,
            data,
            username,
            password,
            use_async=use_async,
            safety_check=safety_check,
        )
        if results["failed"]:
            fp = os.path.join(output_dir, "failed", f"{entity_type}.json")
            utils.write_json(results["failed"], fp)
            logger.info(
                f"❌ Failed to delete {len(results['failed'])} {entity_type}"
            )

        if results["success"]:
            fp = os.path.join(output_dir, "success", f"{entity_type}.json")
            utils.write_json(results["success"], fp)
            logger.info(f"✅ Deleted {len(results['success'])} {entity_type}")
    else:
        logger.info(f"0️⃣  No {endpoint} resources to delete. Aborting")

    logger.info(
        f"⏰ Elapsed time (hh:mm:ss): {utils.elapsed_time_hms(start_time)}"
    )
    logger.info(f"📝  Wrote FHIR delete results for {study_id} to {output_dir}")
    logger.info(f"✅ Completed FHIR delete for {entity_type}")

    return results


def delete_all(
    base_url,
    entity_types=None,
    output_dir=None,
    study_id=None,
    username=None,
    password=None,
    use_async=True,
    safety_check=True,
    use_kf_entity_tags=True,
):
    """
    Delete all FHIR resources in the server by type. If study_id is provided
    then delete only the resources which have been tagged by that study

    :param base_url: Base url of the FHIR service
    :type base_url: str
    :param entity_types: Kids First FHIR entity types in
    d3b_api_client_cli.config.KidsFirstFhirEntity
    :type entity_types: str
    :param study_id: the study_id to filter resources to delete
    :type study_id: str
    :param username: username of the API admin user
    :type username: str
    :param password: password of the API admin user
    :type password: str
    :param use_async: A flag to determine whether to use multi-threading when
    sending requests to the server
    :type use_async: boolean
    :param safety_check: A flag to prevent deleting in non-local servers
    :type safety_check: boolean
    :param use_kf_entity_tags: A flag to include the KF entity type in the
    query params. This is used when deleting from the new FHIR servers
    :type use_kf_entity_tags: boolean

    :returns: None
    """
    if safety_check and (not utils.is_localhost(base_url)):
        raise Exception(
            f"❌ Cannot delete from {base_url} because safety_check is ENABLED. "
            f"Resources that are not in {LOCAL_HOSTS} will not be deleted "
            "unless you set safety_check=False."
        )

    if not (username and password):
        username = config["username"]
        password = config["password"]

    if not output_dir:
        last_dir = study_id if study_id else "all"
        output_dir = os.path.join(DELETE_DIR, last_dir)
    os.makedirs(os.path.join(output_dir, "success"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "failed"), exist_ok=True)

    start_time = time.time()
    failed = {}
    success = {}
    for entity_type in entity_delete_order:
        if entity_types and (entity_type not in set(entity_types)):
            logger.info(
                f"Skipping {entity_type}, not in filters:"
                f" {pformat(entity_types)}"
            )
            continue

        # Delete all KF FHIR entities (e.g. vital_status)
        # KF FHIR entity is translated into a FHIR resource query
        # e.g. vital_status -> /Observation?code=250537006&_tag=<study_id>

        # Specimens need to be deleted in a different way since their
        # references to each other are not known ahead of time and therefore
        # we don't know the order of deletion
        if entity_type.endswith("specimen"):
            results = _delete_specimens(
                base_url,
                output_dir,
                study_id,
                username,
                password,
                use_async,
                safety_check,
            )
        else:
            results = delete_all_of_type(
                base_url,
                entity_type,
                output_dir,
                study_id=study_id,
                username=username,
                password=password,
                use_async=use_async,
                safety_check=safety_check,
                use_kf_entity_tags=use_kf_entity_tags,
            )
        failed[entity_type] = len(results["failed"])
        success[entity_type] = len(results["success"])

    logger.info(
        f"⏰ Elapsed time (hh:mm:ss): {utils.elapsed_time_hms(start_time)}"
    )
    if any(failed.values()):
        logger.info(f"❌ Failed delete counts:\n{pformat(failed)}")
    if any(success.values()):
        logger.info(f"✅ Success delete counts:\n{pformat(success)}")
    if not (any(failed.values()) or any(success.values())):
        logger.info("0️⃣  This study had no data to delete!")
    logger.info(
        f"✅ Completed FHIR delete for study {study_id if study_id else ''}"
    )


def delete_from_file(
    base_url,
    data_dir,
    entity_types=None,
    username=None,
    password=None,
    use_async=True,
    safety_check=True,
):
    """
    Read FHIR json files and delete the resources by ID in FHIR service

    :param base_url: Base url of the FHIR service
    :type base_url: str
    :param data_dir: Dir where data is loaded from
    :type data_dir: str
    :param entity_types: Kids First FHIR entity types in
    d3b_api_client_cli.config.KidsFirstFhirEntity
    :type entity_types: str
    :param username: username of the API admin user
    :type username: str
    :param password: password of the API admin user
    :type password: str
    :param use_async: A flag to determine whether to use multi-threading when
    sending requests to the server
    :type use_async: boolean
    :param safety_check: A flag to prevent deleting in non-local servers
    :type safety_check: boolean
    :returns: None
    """
    if safety_check and (not utils.is_localhost(base_url)):
        raise Exception(
            f"❌ Cannot delete from {base_url} because safety_check is ENABLED. "
            f"Resources that are not in {LOCAL_HOSTS} will not be deleted "
            "unless you set safety_check=False."
        )

    if not (username and password):
        username = config["username"]
        password = config["password"]

    start_time = time.time()
    for entity_type in entity_delete_order:
        if entity_types and (entity_type not in set(entity_types)):
            logger.info(
                f"Skipping {entity_type}, not in filters:"
                f" {pformat(entity_types)}"
            )
            continue

        filename = f"{entity_type}.json"
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            logger.warning(
                f"⚠️  Skipping {entity_type}, data file does not exist"
            )
            continue

        data = utils.read_json(filepath)
        if not data:
            logger.info(f"0 {entity_type} resources to delete. Aborting")
            continue
        logger.info(f"🗑️ Begin deleting {entity_type} {len(data)} resources")
        results = delete_entities(
            base_url,
            data,
            username,
            password,
            use_async=use_async,
            safety_check=safety_check,
        )

    logger.info(
        f"⏰ Elapsed time (hh:mm:ss): {utils.elapsed_time_hms(start_time)}"
    )
    logger.info("✅ Completed delete")

    return results
