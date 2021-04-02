from databricks_cli.runs.api import RunsApi
from databricks_cli.workspace.api import WorkspaceApi
import os
import time
import base64


class PipelineRun:
    def __init__(self, waiter, run_id):
        self._waiter = waiter
        self._run_id = run_id

    def wait_for_completion(self, show_output=True, timeout_seconds=3600):
        self._waiter(self._run_id, show_output, timeout_seconds)


def PipelineException(Exception):
    """An exception thrown when a pipeline fails."""


class Pipeline:
    def __init__(self, workspace, steps: list):
        self._workspace = workspace
        self._steps = steps
        self._runs = RunsApi(self._workspace._client)

    def validate(self):
        return True

    def publish(self, name, description, version):
        header = [
            "# Databricks notebook source\n",
            "# MAGIC %md\n",
            f"# MAGIC # {name} ({version})\n",
            f"# MAGIC {description}\n",
            "\n",
            "# COMMAND ----------\n",
            "\n",
        ]
        lines = []
        for step in self._steps:
            source = os.path.join(step.source_directory, step.script_name)
            with open(source, "r") as f:
                lines.append(f"# DBTITLE 1,{step.name}\n")
                for line in f.readlines():
                    lines.append(line)
                lines.append("\n")
                lines.append("# COMMAND ----------\n")
                lines.append("\n")
            step.runconfig.environment.install_libraries(step.compute_target)

        self._header = header
        self._lines = lines

    def submit(self, experiment_name) -> PipelineRun:
        workspace_client = WorkspaceApi(self._workspace._client)
        workspace_client.mkdirs("/Pipelines")
        workspace_client.mkdirs("/Experiments")

        lines = self._header
        lines += [
            "import mlflow\n",
            f'mlflow.set_experiment("/Experiments/{experiment_name}")\n',
            "\n",
            "# COMMAND ----------\n",
            "\n",
        ]
        lines += self._lines
        notebook = str.join("", lines)

        path = f"/Pipelines/{experiment_name}"
        rsp = self._workspace._client.perform_query(
            method="POST",
            path="/workspace/import",
            data={
                "content": base64.b64encode(notebook.encode("utf-8")).decode(
                    "utf-8"
                ),
                "path": path,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE",
            },
        )

        cluster_id = self._steps[0].compute_target.get("cluster_id")
        json = {
            "existing_cluster_id": cluster_id,
            "notebook_task": {"notebook_path": path},
        }

        rsp = self._runs.submit_run(json)
        return PipelineRun(
            lambda r, o, t: self.wait(r, o, t), rsp.get("run_id")
        )

    def wait(self, run_id, show_output, timeout_seconds):
        while timeout_seconds > 0:
            rsp = self._runs.get_run(str(run_id))
            state = rsp.get("state", {}).get("life_cycle_state")
            if (
                show_output is not None
                and isinstance(show_output, str)
                and show_output.lower() == "full"
            ):
                print(rsp)
            elif show_output:
                print(state)
            if state == "SKIPPED":
                raise PipelineException("Job skipped")
            if state == "INTERNAL_ERROR":
                reason = rsp.get("reason")
                raise PipelineException(f"Internal Error: {reason}")
            if state == "TERMINATED":
                if rsp.get("state", {}).get("result_state") == "FAILED":
                    run_url = rsp.get("run_page_url")
                    raise PipelineException(
                        f"Execution failed: run_page_url={run_url}"
                    )
                break
            timeout_seconds -= 5
            time.sleep(5)

        if state != "TERMINATED" and timeout_seconds <= 0:
            run_url = rsp.get("run_page_url")
            raise PipelineException(
                f"Execution timed out: run_page_url={run_url}"
            )

        output = self._runs.get_run_output(str(run_id))
        if (
            show_output is not None
            and isinstance(show_output, str)
            and show_output.lower() == "full"
        ):
            print(output)
        elif show_output:
            print(output.get("notebook_output").get("result"))
