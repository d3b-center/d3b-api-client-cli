"""
Test generating fake data for global ID commands
"""
import pytest
from click.testing import CliRunner
import pandas

from d3b_api_client_cli.cli.faker.global_id_commands import *
from d3b_api_client_cli.faker.global_id import (
    generate_global_id_file as _generate_global_id_file,
    DEFAULT_FHIR_RESOURCE_TYPE
)


@pytest.mark.parametrize(
    "kwargs,error_msg",
    [
        (
            {
                "fhir_resource_type": "foo"
            }, "BadParameter"
        )
    ]
)
def test_generate_global_ids_errors(kwargs, error_msg):
    """
    Test generate_global_id_file errors
    """
    runner = CliRunner()
    result = runner.invoke(
        generate_global_id_file,
        ["--fhir-resource-type", "foo"],
        standalone_mode=False,
    )
    assert result.exit_code == 1
    assert error_msg in str(result.exc_info)


def test_generate_global_ids(tmp_path):
    """
    Test generate_global_id_file
    """
    temp_dir = tmp_path / "output"
    temp_dir.mkdir()

    # With global IDs
    filepath = _generate_global_id_file(
        starting_index=250,
        output_dir=temp_dir
    )
    df = pandas.read_csv(filepath)

    for c in ["fhirResourceType", "descriptor", "globalId"]:
        assert c in df.columns

    assert df["fhirResourceType"].eq(DEFAULT_FHIR_RESOURCE_TYPE).all()
    assert df["descriptor"].apply(
        lambda d: int(d.split("-")[-1])
    ).ge(250000).all()

    # Without global IDs
    filepath = _generate_global_id_file(
        output_dir=temp_dir,
        with_global_ids=False
    )
    df = pandas.read_csv(filepath)
    assert "globalId" not in df.columns
    assert df["descriptor"].apply(
        lambda d: int(d.split("-")[-1])
    ).ge(0).all()
    assert df["descriptor"].apply(
        lambda d: int(d.split("-")[-1])
    ).le(9000000000).all()
