from databricks_cli.sdk.api_client import ApiClient
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
        client = ApiClient(
            host=self._workspace_url,
            token=self._aad_token
            if self._aad_token is not None
            else self._access_token,
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
