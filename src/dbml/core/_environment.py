from databricks_cli.libraries.api import LibrariesApi
from databricks_cli.clusters.api import ClusterApi


class Environment:
    def __init__(self, name, requirements):
        self._name = name
        self._requirements = requirements

    @staticmethod
    def from_requirements(name, requirements):
        return Environment(name, requirements)

    def register(self, workspace):
        self._workspace = workspace

    def install_libraries(self, compute_target):
        with open(self._requirements, "r") as f:
            libraries = [{"pypi": {"package": p}} for p in f.readlines() if p]
            if len(libraries) > 0:
                cluster_id = compute_target.get("cluster_id")
                cluster_client = ClusterApi(self._workspace._client)
                cluster_client.start_cluster(cluster_id)
                library_client = LibrariesApi(self._workspace._client)
                library_client.install_libraries(cluster_id, libraries)
