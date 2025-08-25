# Databricks notebook source
# MAGIC %pip install /dbfs/FileStore/package/cmapss_pipeline_0_4_0.tar.gz --force-reinstall

# COMMAND ----------

from cmapss_pipeline.medallion.bronze_to_silver import SilverProcessor
from pyspark.sql import SparkSession

dbutils.widgets.text("bronze_path", "/mnt/adls/bronze")
dbutils.widgets.text("silver_path", "/mnt/adls/silver")

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")

processor = SilverProcessor(spark, bronze_path, silver_path)
processor.run_all()
