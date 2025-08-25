from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, max as spark_max, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

class SilverProcessor:
    """
    Clase responsable de transformar los datos de la capa Bronze a la capa Silver.
    Incluye cálculo del Remaining Useful Life (RUL) y normalización de sensores.
    """

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        """
        Inicializa el procesador Silver.

        Args:
            spark (SparkSession): Sesión de Spark.
            bronze_path (str): Ruta de entrada para los datos en la capa Bronze.
            silver_path (str): Ruta de salida para los datos en la capa Silver.
        """
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.stats_dict = {}

    def _load_data(self):
        """
        Carga los datasets desde la capa Bronze:
        - df_train: histórico de entrenamiento.
        - df_test: secuencias incompletas de prueba.
        - df_rul: RUL real para los motores del test.
        """
        self.df_train = self.spark.read.parquet(f"{self.bronze_path}/train")
        self.df_test = self.spark.read.parquet(f"{self.bronze_path}/test")
        self.df_rul = self.spark.read.parquet(f"{self.bronze_path}/RUL")

    def _calcular_rul_train(self):
        """
        Calcula el RUL (Remaining Useful Life) para cada fila del dataset de entrenamiento.
        RUL = ciclo máximo del motor - ciclo actual.
        """
        w = Window.partitionBy("engine_id")
        self.df_train = self.df_train.withColumn("max_cycle", spark_max("cycle").over(w))
        self.df_train = self.df_train.withColumn("RUL", (col("max_cycle") - col("cycle")).cast("int"))
        self.df_train = self.df_train.drop("max_cycle")

    def _asignar_rul_test(self):
        """
        Asigna el RUL a la última fila de cada motor en el dataset de test, según el archivo RUL externo.
        """
        self.df_rul = self.df_rul.withColumn("engine_id", monotonically_increasing_id() + 1)
        w = Window.partitionBy("engine_id").orderBy(col("cycle").desc())
        df_test_last = self.df_test.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
        df_test_last = df_test_last.join(self.df_rul, on="engine_id", how="left")
        self.df_test = self.df_test.join(df_test_last.select("engine_id", "cycle", "RUL"), on=["engine_id", "cycle"], how="left")

    def _normalizar_sensores(self):
        """
        Normaliza las columnas de sensores del dataset (media 0, desviación típica 1).
        Las estadísticas se calculan a partir del dataset de entrenamiento y se aplican a ambos datasets.
        """
        sensor_cols = [c for c in self.df_train.columns if c.startswith("sensor_")]

        for sensor in sensor_cols:
            stats = self.df_train.select(mean(sensor).alias("mean"), stddev(sensor).alias("std")).first()
            self.stats_dict[sensor] = {"mean": stats["mean"], "std": stats["std"]}

        for sensor in sensor_cols:
            mean_val = self.stats_dict[sensor]["mean"]
            std_val = self.stats_dict[sensor]["std"]
            if std_val != 0 and std_val is not None:
                self.df_train = self.df_train.withColumn(sensor, (col(sensor) - mean_val) / std_val)
                self.df_test = self.df_test.withColumn(sensor, (col(sensor) - mean_val) / std_val)

    def _guardar_silver(self):
        """
        Guarda los datasets procesados en formato Parquet en la capa Silver.
        """
        self.df_train.write.mode("overwrite").parquet(f"{self.silver_path}/train")
        self.df_test.write.mode("overwrite").parquet(f"{self.silver_path}/test")

    def run_all(self):
        """
        Ejecuta la pipeline completa de transformación a nivel Silver.
        """
        print("Iniciando transformación a Silver...")
        self._load_data()
        self._calcular_rul_train()
        self._asignar_rul_test()
        self._normalizar_sensores()
        self._guardar_silver()
        print("✅ Transformación completada.")
