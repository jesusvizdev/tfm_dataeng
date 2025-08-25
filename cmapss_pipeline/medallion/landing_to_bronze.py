from pyspark.sql import SparkSession
from pyspark.sql.functions import split, trim, regexp_replace, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id


class BronzeProcessor:
    """
    Clase encargada de procesar archivos de texto crudos del dataset CMAPSS
    y convertirlos en DataFrames estructurados listos para su uso en la capa Bronze.

    Funcionalidades:
    - Limpieza y parsing del archivo de texto.
    - Asignación de nombres de columnas significativos.
    - Aplicación de tipos de datos adecuados.
    - Escritura en formato Parquet en la capa Bronze.
    """

    COLUMN_TYPES = (
            [("engine_id", IntegerType()), ("cycle", IntegerType())] +
            [(f"op_setting_{i}", DoubleType()) for i in range(1, 4)] +
            [(f"sensor_{i}", DoubleType()) for i in range(1, 22)]
    )

    def __init__(self, spark: SparkSession, landing_path: str, bronze_path: str):
        """
        Inicializa el procesador Bronze.

        Args:
            spark (SparkSession): Sesión activa de Spark.
            landing_path (str): Ruta de entrada donde están los archivos txt.
            bronze_path (str): Ruta de salida para guardar los datos transformados en Parquet.
        """
        self.spark = spark
        self.landing_path = landing_path
        self.bronze_path = bronze_path


    def _read_and_clean_raw(self, file_path: str):
        """
        Lee un archivo de texto crudo y lo transforma en un DataFrame de columnas separadas.

        Pasos:
        - Elimina comillas y espacios adicionales.
        - Sustituye espacios por punto y coma como delimitador.
        - Divide la línea en columnas individuales.

        Args:
            file_path (str): Ruta completa del archivo a procesar.
        Returns:
            DataFrame con columnas divididas.
        """
        df_raw = self.spark.read.text(file_path)
        df_clean = df_raw.withColumn("value", regexp_replace(trim(col("value")), r"\"+", ""))
        df_clean = df_clean.withColumn("value", regexp_replace(col("value"), r"\s+", ";"))
        df_split = df_clean.withColumn("columns", split(col("value"), ";"))

        for i in range(26):
            df_split = df_split.withColumn(f"col_{i}", col("columns")[i])

        return df_split.drop("value", "columns")

    def _rename_filter_columns(self, df):
        """
        Renombra las columnas del DataFrame a nombres descriptivos y filtra filas inválidas.

        Args:
            df: DataFrame crudo con nombres de columna genéricos.
        Returns:
            DataFrame con nombres correctos y sin filas vacías.
        """
        column_names = (
                ["engine_id", "cycle", "op_setting_1", "op_setting_2", "op_setting_3"] +
                [f"sensor_{i}" for i in range(1, 22)]
        )

        for i, new_name in enumerate(column_names):
            df = df.withColumnRenamed(f"col_{i}", new_name)

        df = df.withColumn("row_id", monotonically_increasing_id())
        df_filtered = df.filter(col("row_id") > 0).drop("row_id")
        return df_filtered

    def _apply_schema(self, df):
        """
        Aplica tipos de datos definidos a cada columna.

        Args:
            df: DataFrame con nombres de columna correctos.
        Returns:
            DataFrame con columnas casteadas a sus tipos adecuados.
        """
        for col_name, col_type in BronzeProcessor.COLUMN_TYPES:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
        return df

    def process_train(self):
        """
        Procesa el archivo de entrenamiento (train_FD001.txt)
        y lo guarda como Parquet en la ruta de Bronze.
        """
        print("- Procesando train_FD001...")
        df_cleaned = self._read_and_clean_raw(f"{self.landing_path}/train_FD001.txt")
        df_renamed = self._rename_filter_columns(df_cleaned)
        df_schema = self._apply_schema(df_renamed)
        df_schema.write.mode("overwrite").parquet(f"{self.bronze_path}/train")

    def process_test(self):
        """
        Procesa el archivo de prueba (test_FD001.txt)
        y lo guarda como Parquet en la ruta de Bronze.
        """
        print("- Procesando test_FD001...")
        df_cleaned = self._read_and_clean_raw(f"{self.landing_path}/test_FD001.txt")
        df_renamed = self._rename_filter_columns(df_cleaned)
        df_schema = self._apply_schema(df_renamed)
        df_schema.write.mode("overwrite").parquet(f"{self.bronze_path}/test")

    def process_rul(self):
        """
        Procesa el archivo de RUL (RUL_FD001.txt)
        y lo guarda como Parquet en la ruta de Bronze.
        """
        print("- Procesando RUL_FD001...")
        df = self.spark.read.text(f"{self.landing_path}/RUL_FD001.txt")
        df = df.withColumn("row_id", monotonically_increasing_id())
        df = df.filter(col("row_id") > 0).drop("row_id")
        df = (
            df
            .withColumnRenamed("value", "RUL")
            .withColumn("RUL", regexp_replace(trim(col("RUL")), r"\"+", ""))
            .withColumn("RUL", col("RUL").cast(IntegerType()))
        )
        df.write.mode("overwrite").parquet(f"{self.bronze_path}/RUL")

    def run_all(self):
        """
        Ejecuta todo el pipeline de transformación Bronze:
        - train
        - test
        - RUL
        """
        print("Iniciando transformación a Bronze...")
        self.process_train()
        self.process_test()
        self.process_rul()
        print("✅ Transformación completada.")
