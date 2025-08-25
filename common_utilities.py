# Databricks notebook source
# MAGIC %md
# MAGIC ### **Common Imports**

# COMMAND ----------

from pyspark.sql.functions import *
import os
from datetime import datetime
import re


# COMMAND ----------


# Read multiple CSV files from a given volume path
def read_multiple_csvs(volume_path):
    return spark.read.option("header", True).option("inferSchema", True).csv(volume_path)


# COMMAND ----------

def write_to_delta_table(df, full_table_name,mode=mode):
    df.write.format("delta").mode(mode).saveAsTable(full_table_name)


# COMMAND ----------

# Convert column names to snake_case
def to_snake_case_df(df):
    def to_snake(col_name):
        col_name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', col_name)
        col_name = re.sub(r'[^a-zA-Z0-9]', '_', col_name)
        return col_name.lower().strip('_')
    
    for col_name in df.columns:
        new_col_name = to_snake(col_name)
        if new_col_name != col_name:
            df = df.withColumnRenamed(col_name, new_col_name)
    return df

# COMMAND ----------

def add_current_date(df):
    df = df.withColumn("load_date", current_timestamp())
    return df

# COMMAND ----------

def read_delta_table(catalogname:str, database: str, table_name: str):
    return spark.table(f"{catalogname}.{database}.{table_name}")

