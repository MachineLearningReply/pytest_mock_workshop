import pytest
import copy
import yaml
from pathlib import Path
from azure.storage.blob import BlobClient
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
def mock_yaml_config(monkeypatch):
    monkeypatch.setattr(yaml, "load", lambda *args, **kwargs: copy.deepcopy(YAML_CSV))


@pytest.fixture
def data_conf(mock_yaml_config):
    yield DataConfiguration(copy.deepcopy(YAML_CSV), "test_source", "test_table_name")


def test_check_config_keys_in_init(monkeypatch):
    monkeypatch.delattr(DataConfiguration, "_check_config_keys", raising=True)

    with pytest.raises(AttributeError):
        DataConfiguration({}, "test_source", "test_table_name")

@pytest.mark.parametrize(
    "col_list",
    [[], [{"name": "Col1", "dtype": "int"}], [{"name": "Col1", "dtype": "int"}, {"name": "Col1", "dtype": "str"}]],
)
def test_check_column_config_fail(col_list, data_conf, monkeypatch):
    monkeypatch.setitem(data_conf._config, "columns", col_list)

    with pytest.raises(ConfigurationError):
        data_conf._check_column_config()

@pytest.mark.parametrize(
    "param",
    ["version", "header", "file_format", "columns"],
)
def test_check_config_keys_fail(param, data_conf, monkeypatch):
    monkeypatch.delitem(data_conf._config, param)

    with pytest.raises(ConfigurationError):
        data_conf._check_config_keys()


def test_from_client(mock_blob_client, mock_yaml_config, monkeypatch):

    def mock_from_connection_string(*args, **kwargs):
        return mock_blob_client()

    monkeypatch.setattr(BlobClient, "from_connection_string", mock_from_connection_string)

    sample_client =  Client(connection_string="", container_name="", cloud_file_path="", log_level="DEBUG")
    dcfg = DataConfiguration.from_client(sample_client, Path("source/file_config.yaml"))

    assert dcfg.source == "source"
    assert dcfg.table_name == "file_config.yaml"
