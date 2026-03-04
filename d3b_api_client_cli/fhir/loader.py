"""
Load FHIR JSON into the FHIR server efficiently
"""

import os
import shutil
import time
import logging
from collections import defaultdict
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pprint import pformat

import requests
from requests.auth import HTTPBasicAuth

from d3b_api_client_cli.config import (
    config,
    LOAD_DIR,
    SKIP_ENTITIES,
    KidsFirstFhirEntity,
    ImagingFhirEntity,
)
from d3b_api_client_cli import utils

logger = logging.getLogger(__name__)

config = config["fhir"]
entity_load_order = [et.value for et in KidsFirstFhirEntity] + [
    et.value for et in ImagingFhirEntity
]
valid_fhir_types = set(entity_load_order)


def _do_put(
    base_url, endpoint, username, password, payload, ignore_errors=None
):
    """
    Helper function to PUT FHIR resource
    """
    resource_id = payload["id"]
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
            "put",
            url,
            ignore_status_codes=None,
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            json=payload,
        )
    except requests.exceptions.HTTPError as e:
        results["failed"] = True
        results["resp"] = {"id": resource_id}
        results["error"] = str(e)

        if ignore_errors is False:
            raise
    else:
        results["resp"] = resp.json()

    return results


def _process_result(result, results, i, total, counts, entity_type):
    """
    Helper to process result of _do_put
    """
    url = result.get("url")
    url_path = urlparse(url).path
    resp = result.get("resp")
    failed = result["failed"]

    if entity_type not in counts:
        counts[entity_type] = {"success": 0, "failed": 0}

    if not failed:
        counts[entity_type]["success"] += 1
        results["success"].append(resp)
        logger.info(f"PUT {url_path}, #{i + 1}/{total}")
    else:
        counts[entity_type]["failed"] += 1
        results["failed"].append({"resp": resp, "error": result["error"]})
        logger.info(f"FAILED PUT {url_path} #{i + 1}/{total}")

    return resp


def _write_results(entity_type, results, output_dir):
    """
    Write put results to file
    """
    if results["failed"]:
        fp = os.path.join(output_dir, "failed", f"{entity_type}.json")
        utils.write_json(results["failed"], fp)
        logger.info(
            f"❌ Failed to update {len(results['failed'])} {entity_type}"
        )

    if results["success"]:
        fp = os.path.join(output_dir, "success", f"{entity_type}.json")
        utils.write_json(results["success"], fp)
        logger.info(f"✅ Updated {len(results['success'])} {entity_type}")

    logger.info(f"Wrote results to {output_dir}")


def load_data(
    base_url,
    data_dir,
    output_dir=None,
    username=None,
    password=None,
    use_async=True,
    entities_to_load=None,
    ignore_load_errors=False,
    cleanup=False,
):
    """
    Read FHIR json files and load into FHIR service

    :param base_url: Base url of the FHIR service
    :type base_url: str
    :param data_dir: Dir where data is loaded from
    :type data_dir: str
    :param use_async: A flag to determine whether to use multi-threading when
    sending requests to the server
    :param cleanup: if true, delete the contents of ETL stage directories
    before running ETL
    :type cleanup: boolean
    :type use_async: boolean
    :returns: None
    """
    if not output_dir:
        output_dir = LOAD_DIR

    if cleanup:
        shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(os.path.join(output_dir, "success"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "failed"), exist_ok=True)

    start_time = time.time()

    if not entities_to_load:
        entities_to_load = valid_fhir_types
    else:
        entities_to_load = set(entities_to_load)

    # NOTE - This is temporary until Dewrangle is able to support these having
    # these types in multiple studies
    entities_to_load = entities_to_load - SKIP_ENTITIES

    study_id = None
    if not (username and password):
        username = config["username"]
        password = config["password"]

    counts = defaultdict(int)
    for entity_type in entity_load_order:
        if entity_type not in entities_to_load:
            logger.info(
                f"⏭️  Skip {entity_type}. User did not include it in"
                " entities_to_load"
            )
            continue

        # NOTE: VERY IMPORTANT
        # Non-aliquot specimens must be loaded synchronously because they
        # have to be loaded in a specific order which satisfies the specimen
        # tree
        save_async = use_async
        if entity_type == "parent_specimen":
            use_async = False
        else:
            use_async = save_async

        filename = f"{entity_type}.json"
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            logger.warning(
                f"⚠️  Skipping {entity_type}, data file does not exist"
            )
            continue
        data = utils.read_json(filepath)

        if not study_id and (entity_type == "research_study"):
            study_id = data[0]["id"].upper().replace("-", "_")

        results = {"failed": [], "success": []}
        total = len(data)
        logger.info(f"👉 Begin loading {entity_type} {total} entities")

        if use_async:
            logger.info("⚡️ Using async loading ...")
            with ThreadPoolExecutor() as tpex:
                futures = []
                for i, payload in enumerate(data):
                    endpoint = payload["resourceType"]
                    futures.append(
                        tpex.submit(
                            _do_put,
                            base_url,
                            endpoint,
                            username,
                            password,
                            payload,
                            ignore_errors=ignore_load_errors,
                        )
                    )
                for i, f in enumerate(as_completed(futures)):
                    _process_result(
                        f.result(), results, i, total, counts, entity_type
                    )
        else:
            logger.info("🐌 Using synchronous loading ...")
            for i, payload in enumerate(data):
                endpoint = payload["resourceType"]
                result = _do_put(
                    base_url, endpoint, username, password, payload
                )
                _process_result(result, results, i, total, counts, entity_type)

        # Write results
        _write_results(entity_type, results, output_dir)

    logger.info(f"🔢 Load Counts:\n{pformat(dict(counts))}")
    logger.info(
        f"⏰ Elapsed time (hh:mm:ss): {utils.elapsed_time_hms(start_time)}"
    )
    logger.info(f"✅ Completed load")

    return counts
