# Databricks notebook source
# MAGIC %pip install /dbfs/FileStore/package/cmapss_pipeline_0_4_0.tar.gz --force-reinstall

# COMMAND ----------

from cmapss_pipeline.medallion.silver_to_gold import GoldProcessor
from pyspark.sql import SparkSession

dbutils.widgets.text("silver_path", "/mnt/adls/silver")
dbutils.widgets.text("gold_path", "/mnt/adls/gold")

silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

processor = GoldProcessor(spark, silver_path, gold_path)
processor.run_all()

# COMMAND ----------

gold_files = dbutils.fs.ls(gold_path)

for f in gold_files:
    if f.isDir():  
        print(f"Dataset: {f.name}")
        df = spark.read.parquet(f.path)
        df.show(5, truncate=False)