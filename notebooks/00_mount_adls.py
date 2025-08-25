# Databricks notebook source
storage_account_name = "adlstfm"  
container_name = "data"    

# COMMAND ----------

storage_account_name = "adlstfm"  
container_name = "data"    
account_key = dbutils.secrets.get(scope="kv-scope", key="key-adls")
mount_point = "/mnt/adls"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    account_key
)

dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point=mount_point,
    extra_configs={
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": account_key
    }
)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/adls"))