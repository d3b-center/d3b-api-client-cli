"""
Script to initialize the Kids First organization with all studies in Dewrangle

- Upsert organization into Dewrangle
- Create FHIR servers for org
- Fetch all visible studies from Dataservice
- Upsert each study to Dewrangle org
- Attach FHIR servers to each study
"""

import os
import logging
import time
import psycopg2
import psycopg2.extras

from d3b_api_client_cli.config import (
    config,
    ROOT_DATA_DIR,
    DEWRANGLE_FHIR_SERVERS_FILEPATH,
    DATASERVICE_DB_NAME,
    DATASERVICE_DB_HOST,
    DATASERVICE_DB_PORT,
    DATASERVICE_DB_ADMIN_USER,
    DATASERVICE_DB_ADMIN_PW,
)
from d3b_api_client_cli import utils
from d3b_api_client_cli.dewrangle import graphql as gql_client

KF_ORG = {
    "name": "Kids First DRC",
    "visibility": "PRIVATE",
    "description": "The Gabriella Miller Kids First Data Resource Center"
    " is a new, collaborative, pediatric research effort with the goal of"
    " understanding the genetic causes and links between childhood cancer and"
    " structural birth defects",
    "email": "heathap@chop.edu",
    "website": "https://kidsfirstdrc.org",
}
DEFAULT_URL = config["dataservice"]["api_url"]
logger = logging.getLogger(__name__)


def get_studies(kf_id=None, only_visible=True):
    """
    Get all studies or one study by kf_id from db
    """

    try:
        with psycopg2.connect(
            dbname=DATASERVICE_DB_NAME,
            user=DATASERVICE_DB_ADMIN_USER,
            password=DATASERVICE_DB_ADMIN_PW,
            host=DATASERVICE_DB_HOST,
            port=DATASERVICE_DB_PORT,
        ) as conn:
            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                if kf_id:
                    if only_visible:
                        suffix = " and study.visible=true"
                    else:
                        suffix = ""
                    cursor.execute(
                        "select * from study where kf_id= %s" + suffix + ";",
                        (kf_id,),
                    )
                else:
                    if only_visible:
                        suffix = " where study.visible=true"
                    else:
                        suffix = ""

                    cursor.execute("select * from study" + suffix + ";")

                rows = cursor.fetchall()
    except psycopg2.OperationalError as e:
        logger.error(
            "❌ Could not fetch study(ies) due to failure to connect to db:"
            f" {DATASERVICE_DB_HOST}:{DATASERVICE_DB_PORT}/"
            f"{DATASERVICE_DB_NAME}"
            " Check your connection details in the environment"
        )
        raise e

    if kf_id:
        what = f"study {kf_id}"
        if len(rows) == 0:
            raise Exception(
                f"‼️  Could not find study {kf_id} in Dataservice DB"
                f" {DATASERVICE_DB_HOST}:{DATASERVICE_DB_PORT}/"
                f"{DATASERVICE_DB_NAME}. Check study visibility!"
            )
        return rows[0]
    what = "studies"

    logger.info(
        f"🛸 Exported {what} from {DATASERVICE_DB_HOST}:{DATASERVICE_DB_PORT}"
    )

    fp = os.path.join(ROOT_DATA_DIR, "dataservice_studies.json")
    utils.write_json([row["kf_id"] for row in rows], fp)

    logger.info(f"✏️  Wrote exported studies to {fp}")

    return rows


def setup_dewrangle_study(
    kf_study_id,
    fhir_servers,
    upsert_fhir_servers=True,
    dewrangle_org_name=None,
    dewrangle_org_id=None,
):
    """
    Setup Dewrangle study

    - Upsert study into Dewrangle organization
    - Attach FHIR server to study
    """
    start_time = time.time()

    if not (dewrangle_org_id or dewrangle_org_name):
        raise Exception(
            "You must provide either the dewrangle_org_id or dewrangle_org_name"
        )
    # Fetch org
    if dewrangle_org_id:
        org = gql_client.read_organization(dewrangle_org_id=dewrangle_org_id)
    else:
        org = gql_client.read_organization(
            dewrangle_org_name=dewrangle_org_name
        )
    if not org:
        raise Exception(
            f"‼️  Cannot find org ID for '{dewrangle_org_name}'. Aborting "
            "study setup"
        )
    dewrangle_org_id = org["id"]

    # Upsert FHIR servers to org
    if upsert_fhir_servers:
        for fhir_server in fhir_servers:
            result = gql_client.upsert_fhir_server(
                dewrangle_org_id,
                fhir_server,
                oidc_client_secret=fhir_server["authConfig"]["clientSecret"],
            )

    # Fetch study from Dataservice
    study = get_studies(kf_id=kf_study_id)

    # Upsert study into Dewrangle
    input_study = {
        "name": study["name"],
        "globalId": utils.kf_id_to_global_id(kf_study_id),
    }
    result = gql_client.upsert_study(
        input_study, dewrangle_org_id, kf_study_id=kf_study_id
    )

    # Get FHIR servers from Dewrangle org
    org_servers = [edge["node"] for edge in org["fhirServers"]["edges"]]

    end_time = utils.elapsed_time_hms(start_time)
    logger.info(f"⏰ Elapsed time (hh:mm:ss): {end_time}")

    logger.info(
        f"🛠️ ✅ Setup {kf_study_id} study in Dewrangle with"
        f" {len(org_servers)} fhir servers complete!"
    )

    return result


def setup_all_studies(
    fhir_servers, study_ids=None, dewrangle_org_name=None, dewrangle_org_id=None
):
    """
    Setup all Kids First Dataservice studies within an org in Dewrangle
    that's already setup with FHIR servers
    """
    if not (dewrangle_org_id or dewrangle_org_name):
        raise Exception(
            "You must provide either the dewrangle_org_id or dewrangle_org_name"
        )
    # Fetch org
    if not dewrangle_org_id:
        org = gql_client.read_organization(
            dewrangle_org_name=dewrangle_org_name
        )
        if not org:
            raise Exception(
                f"‼️  Cannot find org ID for '{dewrangle_org_name}'. Aborting "
                "study setup"
            )
        else:
            dewrangle_org_id = org["id"]

    # Fetch all studies from dataservice
    studies = get_studies()

    # Only setup studies in the input list
    if study_ids:
        study_ids = set(study_ids)

    # Setup studies
    complete = []
    for i, study in enumerate(studies):
        if study_ids and (study["kf_id"] not in study_ids):
            continue

        logger.info(f"🛠️ Setting up study {i}: {study['kf_id']} ...")
        complete_study = setup_dewrangle_study(
            study["kf_id"],
            fhir_servers,
            upsert_fhir_servers=False,
            dewrangle_org_id=dewrangle_org_id,
        )
        complete.append(complete_study)
    return complete


def setup_dewrangle_org(
    organization_payload=None,
    with_studies=False,
    fhir_servers_filepath=DEWRANGLE_FHIR_SERVERS_FILEPATH,
):
    """
    Script to initialize an organization in Dewrangle. Optionally add all
    Dataservice studies to the organization and run setup process on each

    - Upsert Kids First org
    - Create FHIR servers for org

    if with_studies=True
    - Fetch all visible studies from Dataservice
    - Upsert each study in Dewrangle
    - Attach FHIR servers to each study
    """
    if not os.path.exists(fhir_servers_filepath):
        raise Exception(
            f"‼️  Cannot complete setup without Dewrangle FHIR server config"
            " file. See sampe.fhir_servers.json"
        )
    # Upsert org
    organization_payload = organization_payload or KF_ORG
    result = gql_client.upsert_organization(organization_payload)
    org_id = result["id"]

    # Upsert FHIR servers
    server_configs = utils.read_json(fhir_servers_filepath)
    fhir_servers = []
    for server_config in server_configs:
        result = gql_client.upsert_fhir_server(
            org_id,
            server_config,
            oidc_client_secret=server_config["authConfig"]["clientSecret"],
        )
        fhir_servers.append(result)

    # Fetch all visible studies
    if with_studies:
        studies = setup_all_studies(fhir_servers, dewrangle_org_id=org_id)
        suffix = f"with {len(studies)} studies complete!"
    else:
        suffix = "complete!"

    logger.info(
        f"🛠️ ✅ Setup {organization_payload['name']} organization {suffix}"
    )
