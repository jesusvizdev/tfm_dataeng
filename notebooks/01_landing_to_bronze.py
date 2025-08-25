# Databricks notebook source
# MAGIC %pip install /dbfs/FileStore/package/cmapss_pipeline_0_4_0.tar.gz --force-reinstall

# COMMAND ----------

from cmapss_pipeline.medallion.landing_to_bronze import BronzeProcessor
from pyspark.sql import SparkSession

dbutils.widgets.text("landing_path", "/mnt/adls/landing")
dbutils.widgets.text("bronze_path", "/mnt/adls/bronze")

landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_path")

processor = BronzeProcessor(spark, landing_path, bronze_path)
processor.run_all()