# Databricks notebook source
import joblib
import numpy as np

model_path = "/dbfs/mnt/adls/model/model.pkl"
model = joblib.load(model_path)

# COMMAND ----------

sample =   {'sensor_1': 518.67, 'sensor_2': -2.0702940999657415, 'sensor_3': -1.4296886742871155, 'sensor_4': -0.7153718424063062, 'sensor_5': 14.62, 'sensor_6': -4.61844988863869, 'sensor_7': -0.46016752324861177, 'sensor_8': 0.14873457502012366, 'sensor_9': 0.2907568750874354, 'sensor_10': 1.3, 'sensor_11': 0.13788272423896553, 'sensor_12': 1.463390613030601, 'sensor_13': -1.2969068180274848, 'sensor_14': -1.2933899737994228, 'sensor_15': 2.5280639377994527, 'sensor_16': 0.03, 'sensor_17': -1.05353323569149, 'sensor_18': 2388.0, 'sensor_19': 100.0, 'sensor_20': -2.79681418259235, 'sensor_21': -1.9166293761448205}


# COMMAND ----------

X_df = pd.DataFrame([sample])
pred = model.predict(X_df)[0]
print(f"Predicted RUL: {pred:.2f}")