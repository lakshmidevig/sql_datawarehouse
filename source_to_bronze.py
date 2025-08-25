# Databricks notebook source
# MAGIC %run /Workspace/Users/lakshmi.devigaddam@diggibyte.com/Sql_Datawarehouse/common_utilities

# COMMAND ----------

volume_path = "/Volumes/aws-dms/bronze/sql"

try:
    files = [f for f in dbutils.fs.ls(volume_path) if f.name.endswith(".csv")]
except Exception as e:
    print(f"Failed to list files: {e}")
    files = []

for file in files:
    file_path = file.path
    table_name = file.name.replace(".csv", "").lower()
    full_table_name = f"`aws-dms`.gold.{table_name}_bronze"

    print(f"Processing: {file_path} -> {full_table_name}")

    # Read only this file
    df = read_multiple_csvs(file_path)
    print(f"Schema for {full_table_name}:")
    df.printSchema()

    # Add ingest time
    df = add_current_date(df)
    display(df)

    # Write to Delta table
    write_to_delta_table(df, full_table_name,mode="overwrite")

