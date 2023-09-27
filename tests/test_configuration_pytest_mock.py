import pytest
from pathlib import Path
import copy
from advanced_testing_mock import DataConfiguration
from advanced_testing_mock import ConfigurationError
from advanced_testing_mock import Client

YAML_CSV = {
    "version": "1.0.0",
    "header": True,
    "file_format": "csv",
    "columns": [
        {"name": "Col1", "dtype": "int"},
        {"name": "Col2", "dtype": "str"},
        {"name": "Col3", "dtype": "float"},
    ],
}

@pytest.fixture
def sample_client_pytest_mock(mocker, mock_blob_client):
    mocker.patch("azure.storage.blob.BlobClient.from_connection_string", return_value=mock_blob_client())

    yield Client(connection_string="", container_name="", cloud_file_path="", log_level="DEBUG")


@pytest.fixture
def mock_yaml_config(mocker):
    mocker.patch("yaml.load", return_value=copy.deepcopy(YAML_CSV))


@pytest.fixture
def data_conf(mock_yaml_config):
    yield DataConfiguration(copy.deepcopy(YAML_CSV), "test_source", "test_table_name")


def test_check_config_keys_in_init(monkeypatch):
    monkeypatch.delattr(DataConfiguration, "_check_config_keys", raising=True)

    with pytest.raises(AttributeError):
        DataConfiguration({}, 'test_source', 'test_table_name')


@pytest.mark.parametrize(
    "param",
    ["version", "header", "file_format", "columns"],
)
def test_check_config_keys_fail(param, data_conf, mocker):
    mock_dict = copy.deepcopy(YAML_CSV)
    del mock_dict[param]
    mocker.patch.dict(data_conf._config, mock_dict, clear=True)

    with pytest.raises(ConfigurationError):
        data_conf._check_config_keys()


@pytest.mark.parametrize(
    "col_list",
    [[], [{"name": "Col1", "dtype": "int"}], [{"name": "Col1", "dtype": "int"}, {"name": "Col1", "dtype": "str"}]],
)
def test_check_column_config_fail(col_list, data_conf, mocker):
    mocker.patch.dict(data_conf._config, {"columns": col_list}, clear=False)

    with pytest.raises(ConfigurationError):
        data_conf._check_column_config()


@pytest.mark.parametrize(
    "table_config_fp, expected_source, expected_table_name",
    [
        (Path("source/file_config.yaml"), "source", "file_config.yaml"),
        (Path("source/folder/file_config.yaml"), "source/folder", "file_config.yaml"),
        (Path("source/folder/subfolder/file_config.yaml"), "source/folder/subfolder", "file_config.yaml"),
    ],
)
def test_from_client(
    table_config_fp: Path, expected_source: str, expected_table_name: str, sample_client_pytest_mock, mock_yaml_config
):
    dcfg = DataConfiguration.from_client(sample_client_pytest_mock, table_config_fp)

    assert dcfg.source == expected_source
    assert dcfg.table_name == expected_table_name
