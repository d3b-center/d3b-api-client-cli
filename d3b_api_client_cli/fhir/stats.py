"""
Collect stats like total counts of KF entities in FHIR server
"""

import os
import logging
import urllib.parse

import pandas

from d3b_api_client_cli.config import config
from d3b_api_client_cli.fhir.counts import get_counts

logger = logging.getLogger(__name__)

config = config["fhir"]
FHIR_BASE_URL = config["base_url"]
FHIR_USERNAME = config["username"]
FHIR_PW = config["password"]

fhir_queries = config["mapping"]
dataservice_to_kf_fhir_map = {
    "Participant": ["patient"],
    "Biospecimen": ["child_specimen", "parent_specimen"],
    "Family": ["family"],
    "FamilyRelationship": ["family_relationship"],
    "BiospecimenDiagnosis": ["histopathology"],
    "Phenotype": ["phenotype"],
    "Diagnosis": ["disease"],
    "Outcome": ["vital_status"],
    "GenomicFile": ["drs_document_reference", "drs_document_reference_index"],
}


def count_df(studies, fhir_url, output_dir):
    """
    Write out the entity counts to file
    """
    study_ids = [s["kf_id"] for s in studies]

    # Dataservice entity counts
    fp = os.path.join(output_dir, "fhirservice_counts.csv")
    counts = api_entity_counts(study_ids, fhir_base_url=fhir_url)
    df = pandas.DataFrame(counts)

    # Join Study info with counts
    study_df = pandas.DataFrame(studies)
    df = pandas.merge(df, study_df, left_on="study_id", right_on="kf_id")
    df = df[["study_id", "short_code", "name", "entity_type", "count"]]
    df.to_csv(fp, index=False)

    logger.info(f"✏️  Wrote FHIR service counts to {fp}")

    return df


def api_entity_counts(
    study_ids,
    fhir_base_url=FHIR_BASE_URL,
    username=FHIR_USERNAME,
    password=FHIR_PW,
    legacy_server=True,
):
    """
    Produce a table of entity type counts for each study in FHIR service

    Format:

        --------------------------------------------------
        | Study KF ID | Entity Type | Count | Query String|
        --------------------------------------------------
        | ...                                             |
    """

    logger.info("🔢 Collecting FHIR resource counts ...")

    # We have to do this for legacy server bc the GenomicFile
    # total = drs_document_reference + drs_document_reference_index
    # but in legacy server these two queries are the same since
    # there is no way to differentiate between the two
    # Same is true for Biospecimen. Biospecimen =
    # child_specimen + parent_specimen but the queries don't
    # differentiate
    if legacy_server:
        dataservice_to_kf_fhir_map["Biospecimen"] = ["child_specimen"]
        dataservice_to_kf_fhir_map["GenomicFile"] = ["drs_document_reference"]

    data = []
    for study_id in study_ids:
        logger.info(f"👩🏻‍🔬 Study {study_id} ...")

        # Get KF FHIR entity counts
        counts = get_counts(study_id, legacy_server=legacy_server)
        count_by_type = {item["entity_type"]: item["total"] for item in counts}
        # Map KF FHIR entity counts to Dataservice entity counts
        for entity_type, kf_fhir_entities in dataservice_to_kf_fhir_map.items():
            total = 0
            queries = []
            endpoints = []
            for kf_fhir_entity in kf_fhir_entities:
                cfg = fhir_queries[kf_fhir_entity]
                queries.append(cfg["params"])
                endpoints.append(cfg["endpoint"])
                total += count_by_type[kf_fhir_entity]

            stats = {
                "study_id": study_id,
                "entity_type": entity_type,
                "count": total,
                "url": fhir_base_url,
                "endpoint": ",".join(endpoints),
                "query": ",".join(
                    urllib.parse.urlencode(params) for params in queries
                ),
            }
            data.append(stats)
            logger.info(f"{entity_type} count: {stats['count']}")
    return data
