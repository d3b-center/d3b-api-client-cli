import os

from pprint import pprint
import pandas
import pytest
from click.testing import CliRunner

from d3b_api_client_cli import utils
from d3b_api_client_cli.config import config, ROOT_DIR
from d3b_api_client_cli.cli import *


@pytest.fixture(scope="session")
def global_id_request(tmp_path_factory, dewrangle_study):
    """
    Fixture that requests global IDs from Dewrangle
    """
    global_id = dewrangle_study["globalId"]
    kf_id = utils.global_id_to_kf_id(global_id)
    output_dir = str(tmp_path_factory.mktemp("output"))
    input_filepath = os.path.join(output_dir, "global_id_request.csv")

    df = pandas.DataFrame(
        [
            {"descriptor": f"P{i}", "fhirResourceType": "Patient"}
            for i in range(5)
        ]
    )
    df.to_csv(input_filepath, index=False)

    runner = CliRunner()
    result = runner.invoke(
        upsert_global_ids,
        [kf_id, input_filepath],
        standalone_mode=False,
    )
    assert result.exit_code == 0

    return kf_id, df


def test_upsert_global_ids(global_id_request):
    """
    Test `dwds dewrangle request-global-ids` command
    """
    pass


def test_download_global_ids(tmp_path_factory, global_id_request):
    """
    Test `dwds dewrangle request-global-ids` command
    """
    kf_id, df_ids = global_id_request
    output_dir = str(tmp_path_factory.mktemp("output"))
    filepath = os.path.join(output_dir, "downloaded_global_ids.csv")

    runner = CliRunner()
    result = runner.invoke(
        download_global_ids,
        [kf_id, filepath],
        standalone_mode=False,
    )
    assert result.exit_code == 0

    df = pandas.read_csv(filepath)
    assert utils.df_exists(df)
    assert df[df["fhirResourceType"] == "Patient"].shape[0] == df_ids.shape[0]
