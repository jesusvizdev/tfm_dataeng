import joblib
import os
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
class RULRandomForestTrainer:
    def __init__(self, spark_session, train_path: str, test_path: str, model_save_path: str = None, random_state=42):
        """
        Constructor de la clase

        :param spark_session: sesión Spark activa para leer datos
        :param train_path: ruta parquet para dataset de entrenamiento
        :param test_path: ruta parquet para dataset de test
        :param model_save_path: ruta para guardar modelo entrenado (opcional)
        :param random_state: semilla para reproducibilidad
        """
        self.spark = spark_session
        self.train_path = train_path
        self.test_path = test_path
        self.model_save_path = model_save_path
        self.random_state = random_state

        self.model = None
        self.X_train = None
        self.y_train = None
        self.X_test = None
        self.y_test = None
        self.y_pred = None

    def load_data(self):
        """Carga datos desde Spark, convierte a pandas y prepara datasets"""
        df_train = self.spark.read.parquet(self.train_path).toPandas()
        df_test = self.spark.read.parquet(self.test_path).toPandas()

        df_test = df_test.copy()
        if "RUL" not in df_test.columns:
            df_test["RUL"] = 0

        self.X_train = df_train.drop(columns=["RUL"])
        self.y_train = df_train["RUL"]

        self.X_test = df_test.drop(columns=["RUL"])
        self.y_test = df_test["RUL"]

    def train(self, param_grid=None, cv=3):
        if self.X_train is None or self.y_train is None:
            raise RuntimeError("Datos no cargados. Ejecuta load_data() primero.")

        if param_grid is None:
            param_grid = {
                'n_estimators': [100, 200],
                'max_depth': [10, 20],
                'min_samples_split': [2, 5],
                'min_samples_leaf': [1, 2]
            }

        rf = RandomForestRegressor(random_state=self.random_state)
        grid_search = GridSearchCV(
            rf,
            param_grid,
            cv=cv,
            scoring='neg_root_mean_squared_error',
            n_jobs=-1
        )

        print("Entrenando modelo optimizado con GridSearchCV...")
        grid_search.fit(self.X_train, self.y_train)
        self.model = grid_search.best_estimator_
        print(f"Mejor combinación: {grid_search.best_params_}")

    def predict(self):
        """Realiza predicción sobre test set"""
        if self.model is None:
            raise RuntimeError("Modelo no entrenado. Ejecuta train() primero.")
        self.y_pred = self.model.predict(self.X_test)
        return self.y_pred

    def evaluate_train(self):
        """Evalúa el modelo en el conjunto de entrenamiento (RMSE)"""
        if self.model is None:
            raise RuntimeError("Modelo no entrenado. Ejecuta train() primero.")
        train_rmse = mean_squared_error(self.y_train, self.model.predict(self.X_train), squared=False)
        print(f"Train RMSE: {train_rmse:.2f}")
        return train_rmse

    def save_model(self, path=None):
        """
        Guarda el modelo en disco.
        Si la ruta es tipo dbfs:/ se convierte a ruta local /dbfs/ para joblib.dump.
        """
        if self.model is None:
            raise RuntimeError("Modelo no entrenado. Ejecuta train() primero.")
        save_path = path if path is not None else self.model_save_path
        if save_path is None:
            raise ValueError("No se especificó ruta para guardar el modelo.")

        if save_path.startswith("dbfs:/"):
            local_path = "/dbfs/" + save_path[len("dbfs:/"):]
        else:
            local_path = save_path

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        joblib.dump(self.model, local_path)
        print(f"Modelo guardado en: {save_path}")

    def plot_predictions(self, n_points=100):
        """Grafica las primeras predicciones en test"""
        if self.y_pred is None:
            raise RuntimeError("No hay predicciones. Ejecuta predict() primero.")
        plt.figure(figsize=(10, 4))
        plt.plot(self.y_pred[:n_points], label="Predicted RUL")
        plt.title("Predicciones de RUL en test set (sin verdad)")
        plt.legend()
        plt.grid(True)
        plt.show()
