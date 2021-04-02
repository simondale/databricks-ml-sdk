from sklearn import tree
import seaborn as sns
import mlflow
import mlflow.sklearn


def train_model():
    with mlflow.start_run(nested=True) as run:
        df = sns.load_dataset("iris")
        x_train = df.loc[
            :, ["sepal_length", "sepal_width", "petal_length", "petal_width"]
        ]
        y_train = df.loc[:, ["species"]]

        alg = tree.DecisionTreeClassifier()
        model = alg.fit(x_train, y_train)

        mlflow.log_param("criterion", model.criterion)
        mlflow.log_param("splitter", model.splitter)

        artifact_path = "model"

        mlflow.sklearn.log_model(
            model, artifact_path=artifact_path,
        )
        model_version = mlflow.register_model(
            f"runs:/{run.info.run_id}/{artifact_path}", "iris"
        )
        mlflow.sklearn.save_model(
            model,
            run.info.run_id,
            serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE,
        )
        return model_version


version = train_model()
dbutils.notebook.exit(version)
