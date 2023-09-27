"""
This module provides an interface for interacting with Azure cloud storage.
"""

import logging
from pathlib import Path
from azure.storage.blob import BlobClient


class Client:
    def __init__(self, connection_string: str, container_name: str, cloud_file_path: str, log_level: str = "WARNING"):
        """
        Initializes the Client object with the provided connection string, container name, and cloud file path.

        Parameters:
            connection_string (str): The connection string to access the cloud storage.
            container_name (str): The name of the container in the cloud storage.
            cloud_file_path (str): The path of the file in the cloud storage.
            log_level (str, optional): The logging level for the Client object. Default is "WARNING".
        """

        self._logger: logging.Logger = logging.getLogger("Client")
        self._logger.setLevel(log_level)

        self._client = BlobClient.from_connection_string(
            conn_str=connection_string, container_name=container_name, blob_name=cloud_file_path
        )

        self.container_name = container_name
        self.cloud_file_path = cloud_file_path

    def download_data(self):
        """
        Downloads the data from the Azure cloud file as bytes.

        Returns:
            bytes: The downloaded data.
        """

        download_stream = self._client.download_blob()

        return download_stream.readall()

    def download_file(self, file_path: Path):
        """
        Downloads the Azure cloud file and saves it to the specified local file path.

        Parameters:
            file_path (pathlib.Path): The local path where the cloud file will be saved.

        Returns:
            None
        """

        with open(file_path, "wb+") as file:
            blob_data = self._client.download_blob()
            blob_data.readinto(file)

    def upload_file(self, file_path: Path, overwrite=False):
        """
        Uploads a local file to the Azure cloud storage container.

        Parameters:
            file_path (pathlib.Path): The local path of the file to be uploaded.
            overwrite (bool, optional): Whether to overwrite the existing file with the same name in the cloud storage.
            Default is False.

        Returns:
            None
        """

        with open(file_path, "rb") as file:
            self._client.upload_blob(file, overwrite=overwrite)

    def delete_file(self):
        """
        Deletes the file from the Azure cloud storage container.

        Returns:
            None
        """

        self._client.delete_blob()
