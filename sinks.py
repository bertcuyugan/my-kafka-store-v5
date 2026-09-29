"""Upload destinations ("sinks") for the lake uploader.

A sink only needs two methods:
    check()                         → raise if the destination isn't reachable
    upload(local_path, remote_rel)  → copy one file, return the remote path

remote_rel looks like: raw/order-placed/2026-09-25/batch_101500_ab12cd34.parquet
"""
import os

from lake_config import (DATABRICKS_VOLUME_PATH, AZURE_STORAGE_ACCOUNT,
                         AZURE_STORAGE_CONTAINER, AZURE_STORAGE_SAS_TOKEN,
                         AZURE_STORAGE_ACCOUNT_KEY)


class DatabricksVolumeSink:
    """Uploads to a Unity Catalog volume using the Databricks Files API.

    Authenticates with DATABRICKS_HOST + DATABRICKS_TOKEN from .env.
    """
    name = 'volume'

    def __init__(self, volume_root):
        from databricks.sdk import WorkspaceClient
        self.client = WorkspaceClient()
        self.root = volume_root.rstrip('/')
        self._known_dirs = set()

    def check(self):
        self.client.files.get_directory_metadata(self.root)

    def upload(self, local_path, remote_rel):
        target = f"{self.root}/{remote_rel}"
        parent = target.rsplit('/', 1)[0]
        if parent not in self._known_dirs:
            self.client.files.create_directory(parent)   # no-op if it exists
            self._known_dirs.add(parent)
        with open(local_path, 'rb') as f:
            self.client.files.upload(target, f, overwrite=True)
        return target


class AdlsSink:
    """Uploads to an Azure Data Lake Storage Gen2 container.

    Uses a SAS token if provided, otherwise the storage account key.
    """
    name = 'adls'

    def __init__(self, account, container, sas_token='', account_key=''):
        from azure.storage.filedatalake import DataLakeServiceClient
        if not account:
            raise ValueError("AZURE_STORAGE_ACCOUNT is empty in .env")
        credential = sas_token.lstrip('?') if sas_token else account_key
        if not credential:
            raise ValueError("Set AZURE_STORAGE_SAS_TOKEN or AZURE_STORAGE_ACCOUNT_KEY in .env")
        service = DataLakeServiceClient(
            account_url=f"https://{account}.dfs.core.windows.net",
            credential=credential,
        )
        self.fs = service.get_file_system_client(container)
        self.display_root = f"abfss://{container}@{account}.dfs.core.windows.net"

    def check(self):
        # Listing needs the "List" permission on the SAS token
        next(iter(self.fs.get_paths(recursive=False, max_results=1)), None)

    def upload(self, local_path, remote_rel):
        file_client = self.fs.get_file_client(remote_rel)
        with open(local_path, 'rb') as f:
            file_client.upload_data(f, overwrite=True)
        return f"{self.display_root}/{remote_rel}"


def build_sinks(names):
    sinks = []
    for name in names:
        if name == 'volume':
            sinks.append(DatabricksVolumeSink(DATABRICKS_VOLUME_PATH))
        elif name == 'adls':
            sinks.append(AdlsSink(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_CONTAINER,
                                  AZURE_STORAGE_SAS_TOKEN, AZURE_STORAGE_ACCOUNT_KEY))
        else:
            raise ValueError(f"Unknown sink '{name}'. Use 'volume' and/or 'adls'.")
    if not sinks:
        raise ValueError("LAKE_SINKS is empty. Set it to volume, adls, or volume,adls")
    return sinks