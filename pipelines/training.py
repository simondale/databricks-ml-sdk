from dbml.core import Workspace, Environment
from dbml.core.runconfig import RunConfiguration
from dbml.pipeline.steps import PythonScriptStep
from dbml.pipeline.core import Pipeline
import os


def main():
    ws = Workspace.get()

    compute = ws.compute_targets[os.environ.get("DATABRICKS_CLUSTER_ID")]

    env = Environment.from_requirements("databricks", "model/requirements.txt")
    env.register(ws)

    config = RunConfiguration()
    config.environment = env

    train_step = PythonScriptStep(
        name="Train Model",
        script_name="train.py",
        source_directory="model",
        compute_target=compute,
        runconfig=config,
        allow_reuse=True,
        arguments=[],
        output=[],
    )

    steps = [train_step]

    pipeline = Pipeline(workspace=ws, steps=steps)
    pipeline.validate()
    pipeline.publish("training pipeline", "model training pipeline", "1.0")
    pipeline_run = pipeline.submit("training")
    pipeline_run.wait_for_completion(show_output=True, timeout_seconds=3600)


if __name__ == "__main__":
    main()
