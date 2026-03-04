"""
Generate total counts for KF FHIR types
"""

import os
import logging
import random
from pprint import pformat, pprint
import pandas

from d3b_api_client_cli.fhir.get import get
from d3b_api_client_cli.utils import read_json, elapsed_time_hms
from d3b_api_client_cli.config import (
    config,
    KidsFirstFhirEntity,
)

pandas.set_option("display.max_colwidth", None)

config = config["fhir"]
fhir_base_url = config["base_url"]
fhir_username = config["username"]
fhir_password = config["password"]
kf_fhir_types = [et.value for et in KidsFirstFhirEntity]

logger = logging.getLogger(__name__)

queries = config["mapping"]


def get_counts(
    study_id,
    fhir_base_url=fhir_base_url,
    fhir_username=fhir_username,
    fhir_password=fhir_password,
    legacy_server=True,
):
    """
    Check that the total counts match up between the generated FHIR resources
    and the FHIR resources in the legacy FHIR server
    """
    if not (fhir_base_url and fhir_username and fhir_password):
        raise Exception(
            "You must set the FHIR server connection details"
            "in your environment. See .env.sample for variable names"
        )

    results = []
    for kf_fhir_type in kf_fhir_types:
        logger.info(f"🛜  Fetching counts for {kf_fhir_type}")

        query = queries.get(kf_fhir_type)
        url = f"{fhir_base_url}{query['endpoint']}"
        headers = {"Content-Type": "application/json"}

        # Setup query params
        params = query["params"]
        params["_total"] = "accurate"
        if legacy_server:
            tags = []
        else:
            # In new FHIR servers, we tag the resource with the KF entity type
            # and we MUST include this in the query parameters otherwise we
            # won't be able to differentiate between some entities of the
            # same resource type (e.g. drs doc ref vs drs doc ref index which
            # both have resource type DocumentReference)
            tags = [kf_fhir_type]
        tags.append(study_id)
        params["_tag"] = tags

        result = get(
            url, fhir_username, fhir_password, headers=headers, params=params
        )

        results.append(
            {
                "resource_type": query["endpoint"].strip("/"),
                "entity_type": kf_fhir_type,
                "total": result["total"],
            }
        )

    df = pandas.DataFrame(results)
    logger.info(f"🔢 KF FHIR type counts for {fhir_base_url}:\n{df}")

    return results
