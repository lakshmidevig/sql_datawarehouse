# Databricks notebook source
# MAGIC %run /Workspace/Users/lakshmi.devigaddam@diggibyte.com/Sql_Datawarehouse/source_to_bronze

# COMMAND ----------

def transform_crm_cust_info(catalogname:str,database: str, table_name: str):
    df = read_delta_table(catalogname,database, table_name)

    df = (
        df.filter(F.col("cst_id").isNotNull())
          .withColumn("cst_firstname", F.trim("cst_firstname"))
          .withColumn("cst_lastname", F.trim("cst_lastname"))
          .withColumn("cst_marital_status",
                      F.when(F.upper(F.trim("cst_marital_status")) == "S", "Single")
                       .when(F.upper(F.trim("cst_marital_status")) == "M", "Married")
                       .otherwise("n/a"))
          .withColumn("cst_gndr",
                      F.when(F.upper(F.trim("cst_gndr")) == "F", "Female")
                       .when(F.upper(F.trim("cst_gndr")) == "M", "Male")
                       .otherwise("n/a"))
          .withColumn("load_date", F.current_timestamp()) 
    )
    crm_cust_info_df = to_snake_case_df(df)

    return crm_cust_info_df


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

crm_cust_info_df = transform_crm_cust_info("`aws-dms`", "gold", "cust_info_bronze")
display(crm_cust_info_df)
crm_cust_info_df.write.format("delta").mode("overwrite").saveAsTable("`aws-dms`.gold.crm_cust_info_silver")

# COMMAND ----------

df = read_delta_table("`aws-dms`", "gold", "loc_a101_bronze")

def clean_location_info(df):
    return (df
        .withColumn("cntry",
            F.when(F.trim(F.col("cntry")) == "DE", "Germany")
             .when(F.trim(F.col("cntry")).isin("US", "USA"), "United States")
             .when(F.trim(F.col("cntry")) == "", "n/a")
             .when(F.col("cntry").isNull(), "n/a")
             .otherwise(F.trim(F.col("cntry"))))
    )
    


# COMMAND ----------


df = to_snake_case_df(df)
df_silver = clean_location_info(df)
display(df_silver)
df_silver.write.format("delta").mode("overwrite").saveAsTable("`aws-dms`.gold.crm_location_silver")


# COMMAND ----------


sales_df = read_delta_table("`aws-dms`", "gold", "sales_details_bronze")
df_transformed = (sales_df.dropDuplicates(["sls_ord_num"]))
df_transformed.display()


# COMMAND ----------

# Save to silver layer
sales_df = to_snake_case_df(df_transformed)
display(df)
sales_df.write.format("delta").mode("overwrite").saveAsTable("`aws-dms`.gold.sales_details_silver")


# COMMAND ----------

# DBTITLE 1,Clean and Standardize Product Line Column
from pyspark.sql import functions as F

prd_df = read_delta_table("`aws-dms`", "gold", "prd_info_bronze")

final_prd_df = (
    prd_df.withColumn("prd_line", F.upper(F.trim(F.col("prd_line"))))
          .withColumn(
              "prd_line",
              F.when((F.col("prd_line").isNull()) | (F.col("prd_line") == ""), "n/a")
               .otherwise(F.col("prd_line"))
          )
)

display(final_prd_df)

# COMMAND ----------

prd_df= to_snake_case_df(final_prd_df)
prd_df.write.format("delta").mode("overwrite").saveAsTable("`aws-dms`.gold.prd_info_silver")

# COMMAND ----------

cus_az12_df = read_delta_table("`aws-dms`", "gold", "cust_az12_bronze")

def transform_customer_df(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("birth_year", F.year(F.col("bdate")))
          .withColumn("gen", F.upper(F.trim(F.col("gen"))))
          .withColumn(
              "gen",
              F.when(F.col("gen").isin("F", "FEMALE"), "Female")
               .when(F.col("gen").isin("M", "MALE"), "Male")
               .otherwise("n/a")
          )
    )


# COMMAND ----------

final_cus_az12= transform_customer_df(cus_az12_df)
cus_df= to_snake_case_df(final_cus_az12)
cus_df.write.format("delta").mode("overwrite").saveAsTable("`aws-dms`.gold.cust_az12_silver")


# COMMAND ----------

product_catagory_df = read_delta_table("`aws-dms`", "gold", "px_cat_g1v2_bronze")
product_catagory_df.withColumn(
    "ID",
    F.when(F.col("ID").isNull() | (F.col("ID") == ""), "UNKNOWN")
     .otherwise(F.upper(F.trim(F.col("ID"))))
)


# COMMAND ----------

# DBTITLE 1,ble ...

product_df= to_snake_case_df(product_catagory_df)
product_df.display()
product_df.write.format("delta").mode("overwrite").saveAsTable("`aws-dms`.gold.px_cat_g1v2_silver")
