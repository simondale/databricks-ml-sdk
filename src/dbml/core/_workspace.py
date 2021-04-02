from databricks_cli.sdk.api_client import ApiClient
from azure.identity import AzureCliCredential
import os


class Workspace:
    def __init__(self, workspace_url=None, access_token=None, aad_token=None):
        self._workspace_url = (
            workspace_url
            if workspace_url is not None
            else os.environ.get("DATABRICKS_HOST")
        )
        self._access_token = (
            access_token
            if access_token is not None
            else os.environ.get("DATABRICKS_TOKEN")
        )
        self._aad_token = (
            aad_token
            if aad_token is not None
            else os.environ.get("DATABRICKS_AAD_TOKEN")
        )
        self._client = self._get_api_client()

    @staticmethod
    def get(workspace_url=None, access_token=None, aad_token=None):
        return Workspace(workspace_url, access_token, aad_token)

    def _get_api_client(self) -> ApiClient:
        if self._aad_token is None and self._access_token is None:
            cred = AzureCliCredential()
            token = cred.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d").token
        elif self._aad_token is not None:
            token = self._aad_token
        else:
            token = self._access_token

        client = ApiClient(
            host=self._workspace_url,
            token=token,
        )
        return client

    @property
    def compute_targets(self) -> dict:
        return {
            c.get("cluster_id"): c
            for c in self._client.perform_query(
                method="GET", path="/clusters/list"
            ).get("clusters")
        }
