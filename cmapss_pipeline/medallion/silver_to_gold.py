from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class GoldProcessor:
    """
    Clase responsable de transformar los datos desde la capa Silver a la capa Gold,
    dejando los datos listos para el entrenamiento de modelos de predicción de RUL.
    """

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path

    def _load_data(self):
        self.df_train = self.spark.read.parquet(f"{self.silver_path}/train")
        self.df_test = self.spark.read.parquet(f"{self.silver_path}/test")

    def _select_features(self):
        """
        Selecciona únicamente las columnas relevantes: sensores y la columna objetivo RUL.
        Se asume que las columnas 'sensor_*' ya han sido normalizadas en Silver.
        """
        sensor_cols = [c for c in self.df_train.columns if c.startswith("sensor_")]
        self.feature_cols = sensor_cols
        self.target_col = "RUL"

        self.df_train = self.df_train.select(*self.feature_cols, self.target_col)
        self.df_test = self.df_test.select(*self.feature_cols, self.target_col)

    def _save_gold(self):
        """
        Guarda los datasets transformados en la ruta Gold especificada.
        """
        self.df_train.write.mode("overwrite").parquet(f"{self.gold_path}/train")
        self.df_test.write.mode("overwrite").parquet(f"{self.gold_path}/test")

    def run_all(self):
        """
        Ejecuta el pipeline completo Silver → Gold (solo transformación, sin entrenamiento).
        """
        print("Iniciando transformación a Gold...")
        self._load_data()
        self._select_features()
        self._save_gold()
        print("✅ Transformación completada.")
