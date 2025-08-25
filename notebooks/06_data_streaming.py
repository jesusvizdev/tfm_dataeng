# Databricks notebook source
# MAGIC %pip install requests
import os

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import joblib
from pyspark.sql.functions import col

connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")

BASE_PATH = "/mnt/adls/streaming"
SRC_PATH = f"{BASE_PATH}/sensor_rows"
CHK_PATH = f"{BASE_PATH}/checkpoints/sensor_rows"
PRED_PATH = f"{BASE_PATH}/rul_predictions"
PRED_CHK  = f"{BASE_PATH}/checkpoints/rul_predictions"
MODEL_PATH = "/dbfs/mnt/adls/model/model.pkl"

FEATURES = [f"sensor_{i}" for i in range(1, 22)]

ALERT_THRESHOLD = 20.0
FUNC_URL = os.getenv("FUNC_URL")

# COMMAND ----------

ehConf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
}

sensor_schema = StructType()
for i in range(1, 22):  
    sensor_schema = sensor_schema.add(f"sensor_{i}", DoubleType())

df_raw = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

df_json = df_raw.selectExpr("CAST(body AS STRING) as json")

df_parsed = df_json.select(from_json(col("json"), sensor_schema).alias("data")).select("data.*")

# COMMAND ----------

for q in spark.streams.active:
    q.stop()

q_ingest = (
    df_parsed.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", CHK_PATH)
    .trigger(processingTime="5 seconds")
    .start(SRC_PATH)
)

# COMMAND ----------

import requests, time

def send_alert(payload: dict):
    try:
        r = requests.post(FUNC_URL, json=payload, timeout=5)
        if r.status_code != 200:
            print(f"[ALERT] HTTP {r.status_code}: {r.text}")
    except Exception as e:
        print(f"[ALERT] Failed: {e}")

MODEL = joblib.load(MODEL_PATH)

def score_batch(df, epoch_id):
    if df.rdd.isEmpty():
        return
    sdf_feats = df.select([col(c).cast("double").alias(c) for c in FEATURES])
    pdf = sdf_feats.toPandas()
    preds = MODEL.predict(pdf.values)
    pdf["predicted_rul"] = preds
    pdf["processed_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    out_sdf = spark.createDataFrame(pdf)
    (out_sdf.write
      .mode("append")
      .format("delta")
      .option("mergeSchema", "true")  
      .save(PRED_PATH))
    
    low = out_sdf.filter(col("predicted_rul") < ALERT_THRESHOLD)
    for row in low.toLocalIterator():  
        send_alert(row.asDict())

# COMMAND ----------

df_src = spark.readStream.format("delta").load(SRC_PATH)

q_score = (df_src.writeStream
    .foreachBatch(score_batch)
    .option("checkpointLocation", PRED_CHK)
    .outputMode("append")
    .start())

# COMMAND ----------

df_preds = spark.readStream.format("delta").load(PRED_PATH)
display(df_preds)

# COMMAND ----------

