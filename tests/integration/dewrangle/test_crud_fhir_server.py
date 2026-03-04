import os

import pytest
from click.testing import CliRunner

from d3b_api_client_cli.utils import read_json, write_json
from d3b_api_client_cli.config import config, ROOT_DIR, IdTypes
from d3b_api_client_cli.cli import *
from d3b_api_client_cli.dewrangle.graphql import fhir_server


@pytest.fixture(scope="session")
def fhir_server_file(tmp_path_factory):
    """
    Write the inputs to create a Dewrangle FhirServer to file
    """
    data_dir = tmp_path_factory.mktemp("data")
    fhir_server_filepath = os.path.join(data_dir, "FhirServer.json")
    fhir_server = {
        "name": "Test FHIR Server",
        "type": "EXTERNAL",
        "authType": "OIDC_CLIENT_CREDENTIAL",
        "url": "https://kf-api-fhir-service-upgrade-dev.kf-strides.org",
        "authConfig": {
            "clientId": "ingest-study-client",
            "clientSecret": "secret",
            "issuerBaseUrl": "https://kf-keycloak-qa.kf-strides.org/auth/realms/FHIR-TEST",
        },
    }
    write_json(fhir_server, fhir_server_filepath)

    return fhir_server_filepath


def test_crud_fhir_server(tmp_path, fhir_server_file, dewrangle_org):
    """
    Test `dwds dewrangle upsert-fhir-server` command
    Test `dwds dewrangle read-fhir-servers` command
    Test `dwds dewrangle delete-fhir-server` command
    """
    dewrangle_organization_id = dewrangle_org["id"]
    fp = fhir_server_file

    # Upsert
    runner = CliRunner()
    result = runner.invoke(
        upsert_fhir_server,
        [fp, dewrangle_organization_id],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    node_id = result.return_value["id"]
    assert node_id

    # Read
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        read_fhir_servers,
        [dewrangle_organization_id, "--output-dir", temp_dir],
        standalone_mode=False,
    )
    assert result.exit_code == 0
    assert len(result.return_value) > 0
    assert os.path.exists(os.path.join(temp_dir, "FhirServer.json"))

    # Delete
    runner = CliRunner()
    result = runner.invoke(delete_fhir_server, [node_id], standalone_mode=False)
    assert result.exit_code == 0
    assert result.return_value["id"]

    servers = fhir_server.read_fhir_servers(dewrangle_organization_id)
    if servers:
        assert all([server["name"] != "Test FHIR Server" for server in servers])
