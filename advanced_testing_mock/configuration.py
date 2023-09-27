"""
This module contains a class for getting configurations how to process certain data from a YAML file.
"""

import yaml
import logging
from pathlib import Path
from .cloud_interface import Client
from .errors import ConfigurationError


class DataConfiguration:
    def __init__(self, config: dict, source: str, table_name: str, log_level: str = 'WARNING') -> None:
        self._logger: logging.Logger = logging.getLogger("Config")
        self._logger.setLevel(log_level)

        self._config = config
        self.expected_keys = ('version', 'header', 'file_format', 'columns')
        self._check_config_keys()
        self._check_column_config()

        self.source = source
        self.table_name = table_name

        self._logger.debug(f'YAML Config:\n{self._config}')

    @classmethod
    def from_client(cls, client: Client, table_config_fp: Path, log_level: str = 'WARNING'):

        content = client.download_data()
        config = yaml.load(content, Loader=yaml.SafeLoader)
        source = str(table_config_fp.parent)
        table_name = table_config_fp.name

        return cls(config, source, table_name, log_level=log_level)

    @property
    def version(self):
        return self._config['version']

    @property
    def file_format(self):
        return self._config['file_format']


    @property
    def column_names(self):
        if self._config['columns'] is not None:
            names = [col['name'] for col in self._config['columns']]
        else:
            names = []

        return names

    @property
    def column_dtypes(self):
        if self._config['columns'] is not None:
            dtypes = {col['name']: col.get('dtype') for col in self._config['columns'] if col.get('dtype') is not None}
        else:
            dtypes = {}

        return dtypes

    @property
    def header(self):
        if self._config['header']:
            return 'infer'
        else:
            return None


    def _check_config_keys(self):
        if self.expected_keys != tuple(self._config.keys()):
            raise ConfigurationError('There are missing keys in your configuration YAML. Please add these.')

    def _check_column_config(self):
        if len(set(self.column_names)) < 2:
            self._logger.error(f"Column names in file: {self.column_names}")
            self._logger.error(f"Set of column names (no duplicates): {set(self.column_names)}")
            raise ConfigurationError("There are less than two columns in the configuration file.")

        try:
            self.column_dtypes
        except KeyError as e:
            self._logger.error(e)
            raise ConfigurationError('A name for a given column is missing.')


