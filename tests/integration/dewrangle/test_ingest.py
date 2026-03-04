import os

from d3b_api_client_cli.dewrangle import ingest


def _assert_ok(result):
    assert (
        result is not None
    ), "ingest returned None (study lookup/setup failed)"
    assert result.get("status") is True, result


def test_ingest_study_file(fhir_json_data):
    """
    Ingest a single FHIR file into an existing Dewrangle study.
    """
    dewrangle_study_node_id, fhir_json_dir, entities_to_load = fhir_json_data
    fp = os.path.join(fhir_json_dir, "patient.json")

    job = ingest.ingest_study_files(
        fp,
        dewrangle_study_node_id=dewrangle_study_node_id,
        entities_to_load=entities_to_load,
    )

    _assert_ok(job)
    for resource_result in job["job"]["result"]["resources"]:
        assert resource_result["count"] >= 0


def test_ingest_study_files(fhir_json_data):
    """
    Ingest a directory of FHIR files into an existing Dewrangle study.
    """
    dewrangle_study_node_id, fhir_json_dir, entities_to_load = fhir_json_data

    job = ingest.ingest_study_files(
        fhir_json_dir,
        dewrangle_study_node_id=dewrangle_study_node_id,
        entities_to_load=entities_to_load,
    )

    _assert_ok(job)
    for resource_result in job["job"]["result"]["resources"]:
        assert resource_result["count"] >= 0
