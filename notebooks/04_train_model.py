# Databricks notebook source
# MAGIC %pip install /dbfs/FileStore/package/cmapss_pipeline_0_4_0.tar.gz --force-reinstall

# COMMAND ----------

from pyspark.sql import SparkSession
from sklearn.metrics import mean_squared_error
from cmapss_pipeline.model.trainer import RULRandomForestTrainer

dbutils.widgets.text("gold_path", "/mnt/adls/gold")
gold_path = dbutils.widgets.get("gold_path")

train_path  = f"{gold_path}/train"
test_path   = f"{gold_path}/test"
model_path  = "dbfs:/mnt/adls/model/model.pkl"  


trainer = RULRandomForestTrainer(
    spark_session=spark,
    train_path=train_path,
    test_path=test_path,
    model_save_path=model_path,
    random_state=42
)

trainer.load_data()
trainer.train(cv=3) 

y_pred = trainer.predict()


# COMMAND ----------

rmse_train = trainer.evaluate_train()
trainer.save_model()

try:
    trainer.plot_predictions(n_points=100)
except Exception as e:
    print(f"No se pudo graficar: {e}")
