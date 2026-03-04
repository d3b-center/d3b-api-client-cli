import os
import shutil
import pytest
import json
from click.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from d3b_api_client_cli.utils import read_json, send_request
from d3b_api_client_cli.config import (
    ROOT_DIR,
    KF_FHIR_QA_OIDC_CLIENT_SECRET,
    config,
)
from d3b_api_client_cli.cli.fhir.commands import delete_all, load_fhir
from d3b_api_client_cli.cli.dewrangle.graphql_commands import (
    upsert_organization,
    upsert_study,
    upsert_fhir_server,
)
from d3b_api_client_cli.dewrangle.graphql import organization


POSTGRES_DB_IMAGE = "postgres:16-alpine"


def get_study_id():
    """
    Gets a single study from Dataservice

    Used in various tests
    """
    base_url = config["dataservice"]["api_url"]
    url = f"{base_url}/studies?limit=1"
    resp = send_request("get", url)

    return resp.json()["results"][0]["kf_id"]


@pytest.fixture
def fhir_json_data(tmp_path, dewrangle_study):
    """
    Provide a temp FHIR directory + Dewrangle study node id for ingest tests.

    Returns:
      (dewrangle_study_node_id, fhir_dir, entities_to_load)
    """
    src_dir = os.path.join(ROOT_DIR, "tests", "data", "fhir_minimal")
    dest_dir = tmp_path / "fhir_minimal"
    shutil.copytree(src_dir, dest_dir)

    study_json_path = dest_dir / "Study.json"
    with open(study_json_path, "w") as f:
        json.dump({"kf_id": dewrangle_study["globalId"]}, f)

    entities_to_load = ["patient", "observation", "encounter"]

    return (dewrangle_study["id"], str(dest_dir), entities_to_load)


@pytest.fixture(scope="session")
def delete_fhir_data():
    """
    Delete all data in FHIR server
    """
    runner = CliRunner()
    result = runner.invoke(delete_all, [])
    assert result.exit_code == 0


@pytest.fixture(scope="session")
def load_fhirservice(delete_fhir_data, fhir_json_data):
    """
    Load all test data in FHIR server
    """
    study_id, fhir_json_dir = fhir_json_data

    runner = CliRunner()
    result = runner.invoke(load_fhir, [fhir_json_dir])
    assert result.exit_code == 0

    resources = {}
    for fn in os.listdir(fhir_json_dir):
        fp = os.path.join(fhir_json_dir, fn)
        resources[os.path.splitext(fn)[0]] = read_json(fp)

    return study_id, resources


@pytest.fixture(scope="session")
def dewrangle_org():
    """
    Upsert an Organization in Dewrangle for other tests to use
    """
    fp = os.path.join(ROOT_DIR, "tests/data/test-org.json")
    runner = CliRunner()
    result = runner.invoke(upsert_organization, [fp], standalone_mode=False)
    assert result.exit_code == 0

    yield result.return_value

    organization.delete_organization(dewrangle_org_id=result.return_value["id"])


@pytest.fixture(scope="session")
def dewrangle_study(dewrangle_org):
    """
    Upsert a Study in Dewrangle for other tests to use
    """
    fp = os.path.join(ROOT_DIR, "tests/data/test-study.json")
    runner = CliRunner()
    result = runner.invoke(
        upsert_study, [fp, dewrangle_org["id"]], standalone_mode=False
    )
    assert result.exit_code == 0

    study = result.return_value
    study["organization_name"] = dewrangle_org["name"]

    return study


@pytest.fixture(scope="session")
def dewrangle_fhir_server(dewrangle_org):
    """
    Upsert a FHIR server in Dewrangle for other tests to use
    """
    fp = os.path.join(ROOT_DIR, "tests/data/kidsfirst-qa-upgrade-server.json")
    server = read_json(fp)

    runner = CliRunner()
    result = runner.invoke(
        upsert_fhir_server,
        [
            fp,
            dewrangle_org["id"],
            "--oidc-client-secret",
            KF_FHIR_QA_OIDC_CLIENT_SECRET,
        ],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    server["id"] = result.return_value["id"]

    return server


@pytest.fixture(scope="module")
def postgres_db(request):
    """
    Fixture to create Postgres testcontainer for integration testing
    """
    postgres = PostgresContainer(POSTGRES_DB_IMAGE)
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    os.environ["DB_HOST"] = postgres.get_container_host_ip()
    os.environ["DB_PORT"] = postgres.get_exposed_port(5432)
    os.environ["DB_NAME"] = postgres.dbname
    os.environ["DB_USER"] = postgres.username
    os.environ["DB_USER_PW"] = postgres.password
