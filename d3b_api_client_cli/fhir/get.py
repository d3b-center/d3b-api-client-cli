"""
Get FHIR resources from the FHIR server efficiently
"""

import time
import logging
from urllib.parse import urlparse

from requests.auth import HTTPBasicAuth

from d3b_api_client_cli.config import (
    config,
)
from d3b_api_client_cli.utils import (
    elapsed_time_hms,
    send_request,
)

logger = logging.getLogger(__name__)

config = config["fhir"]


def get(url, username, password, headers=None, params=None):
    """
    Helper to GET FHIR resource
    """
    resp = send_request(
        "get",
        url,
        params=params,
        auth=HTTPBasicAuth(username, password),
        headers=headers,
    )
    return resp.json()


def _replace_host_with_origin(base_url, link_url):
    """
    Replace hostname in link_url with base_url hostname if different

    This is needed to support pagination on the legacy FHIR servers since
    some of the legacy servers do not return pagination links with the
    externally resolvable hostname that was requested and,
    instead return http://localhost:8000 or wherever it is deployed behind
    the load balancer
    """
    link_result = urlparse(link_url)
    base_result = urlparse(base_url)
    if link_result.netloc != base_result.netloc:
        link_url = (
            f"{base_result.scheme}://{base_result.netloc}"
            f"{link_result.path}?{link_result.query}"
        )

    return link_url


def get_all(base_url, resource_type, username, password, params=None):
    """Paginate and fetch all FHIR resources by resource_type and
    any query params supplied by caller

    :param base_url: Base url of the FHIR service
    :type base_url: str
    :param resource_type: FHIR endpoint
    :type resource_type: str
    :param params: dict of query params
    :type params: int
    :returns: list of dict responses
    """
    url = "/".join(part.strip("/") for part in [base_url, resource_type])
    headers = {"Content-Type": "application/json"}

    if not params:
        params = {}

    # Get total
    params["_total"] = "accurate"
    total = get(url, username, password, headers, params=params)["total"]

    # Remove this bc getting total can be an expensive FHIR operation
    params.pop("_total")

    logger.info(f"🧺 Begin fetching {resource_type} {total} entities")
    start_time = time.time()

    # Fetch pages of resources
    resources = []
    count = 0
    while count < total:
        params.update({"_count": 100})
        body = get(
            _replace_host_with_origin(base_url, url),
            username,
            password,
            headers=headers,
            params=params,
        )
        # Check if any resources were in the page
        entry = body.get("entry", [])
        if not entry:
            return

        # Add page of resources to output
        page = [entry.get("resource") for entry in body.get("entry", [])]
        resources.extend(page)

        count += int(len(entry))
        logger.info(f"📄 Fetched page of {resource_type}. Seen {count}/{total}")

        # Check if there are any more pages to fetch
        url = None
        links = body.get("link", [])
        for link in links:
            if link["relation"] == "next":
                url = link["url"]
                break
        # This is the last page. Last page link is in self
        if not url:
            for link in links:
                if link["relation"] == "self":
                    url = link["url"]
                    break

    logger.info(f"⏰ Elapsed time (hh:mm:ss): {elapsed_time_hms(start_time)}")
    logger.info("✅ Completed fetching resources")

    return resources
