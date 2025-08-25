from pyspark.sql import SparkSession
from cmapss_pipeline.medallion.landing_to_bronze import BronzeProcessor
from cmapss_pipeline.medallion.bronze_to_silver import SilverProcessor
from cmapss_pipeline.medallion.silver_to_gold import GoldProcessor

def run_pipeline():

    spark = SparkSession.builder \
        .appName("Full CMAPSS Pipeline") \
        .master("local[*]") \
        .getOrCreate()

    landing_path = "data/landing"
    bronze_path = "data/bronze"
    silver_path = "data/silver"
    gold_path = "data/gold"

    print("- Fase 1: Landing → Bronze")
    bronze_processor = BronzeProcessor(spark, landing_path, bronze_path)
    bronze_processor.run_all()

    print("- Fase 2: Bronze → Silver")
    silver_processor = SilverProcessor(spark, bronze_path, silver_path)
    silver_processor.run_all()

    print("- Fase 3: Silver → Gold")
    gold_processor = GoldProcessor(spark, silver_path, gold_path)
    gold_processor.run_all()

    print("✅ Pipeline completo ejecutado.")

    print("\n Mostrando 5 primeras filas de gold/train:")
    df_gold_train = spark.read.parquet(f"{gold_path}/train")
    df_gold_train.show(5, truncate=False)

    print("\n Mostrando 5 primeras filas de gold/test:")
    df_gold_test = spark.read.parquet(f"{gold_path}/test")
    df_gold_test.show(5, truncate=False)

if __name__ == "__main__":
    run_pipeline()