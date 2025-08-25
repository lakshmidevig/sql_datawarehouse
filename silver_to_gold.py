# Databricks notebook source
# MAGIC %run /Workspace/Users/lakshmi.devigaddam@diggibyte.com/Sql_Datawarehouse/bronze_to_silver

# COMMAND ----------

# DBTITLE 1,dim_customers — Dimension Table for Customers
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load Silver tables
crm_cust_info = spark.table("`aws-dms`.gold.crm_cust_info_silver").alias("ci")
erp_cust_az12 = spark.table("`aws-dms`.gold.cust_az12_silver").alias("ca")
erp_loc_a101 = spark.table("`aws-dms`.gold.crm_location_silver").alias("la")

# Join silver tables
dim_customers_df = crm_cust_info \
    .join(erp_cust_az12, crm_cust_info.cst_key == erp_cust_az12.cid, "left") \
    .select(
        crm_cust_info.cst_id.alias("customer_id"),
        crm_cust_info.cst_key.alias("customer_number"),
        crm_cust_info.cst_firstname.alias("first_name"),
        crm_cust_info.cst_lastname.alias("last_name"),
        crm_cust_info.cst_marital_status.alias("marital_status"),
        crm_cust_info.cst_create_date.alias("create_date")
    )


# COMMAND ----------

dim_customers_df.write.mode("overwrite").saveAsTable("`aws-dms`.gold.dim_customers")

# COMMAND ----------

display(dim_customers_df)

# COMMAND ----------

# DBTITLE 1,dim_products — Dimension Table for Products
# Load Silver tables
crm_prd_info = spark.table("silver.crm_prd_info").alias("pn")
erp_px_cat_g1v2 = spark.table("silver.erp_px_cat_g1v2").alias("pc")

# Join Silver tables and filter out historical data (prd_end_dt IS NULL)
dim_products_df = crm_prd_info \
    .join(erp_px_cat_g1v2, crm_prd_info.cat_id == erp_px_cat_g1v2.id, "left") \
    .filter(crm_prd_info.prd_end_dt.isNull()) \
    .select(
        crm_prd_info.prd_id.alias("product_id"),
        crm_prd_info.prd_key.alias("product_number"),
        crm_prd_info.prd_nm.alias("product_name"),
        crm_prd_info.cat_id.alias("category_id"),
        erp_px_cat_g1v2.cat.alias("category"),
        erp_px_cat_g1v2.subcat.alias("subcategory"),
        erp_px_cat_g1v2.maintenance.alias("maintenance"),
        crm_prd_info.prd_cost.alias("cost"),
        crm_prd_info.prd_line.alias("product_line"),
        crm_prd_info.prd_start_dt.alias("start_date")
    )

# Add surrogate key with ROW_NUMBER ordered by start_date and product_number
window_spec = Window.orderBy("start_date", "product_number")

dim_products_df = dim_products_df.withColumn("product_key", F.row_number().over(window_spec)) \
                                 .select("product_key", *[c for c in dim_products_df.columns])

# Register temp view or save table
dim_products_df.createOrReplaceTempView("gold_dim_products")
# Or save permanently:
# dim_products_df.write.mode("overwrite").saveAsTable("gold.dim_products")


# COMMAND ----------



# COMMAND ----------

# Load Silver sales details table
crm_sales_details = spark.table("silver.crm_sales_details").alias("sd")

# Join with gold.dim_products and gold.dim_customers using product_number and customer_id respectively
fact_sales_df = crm_sales_details.alias("sd") \
    .join(dim_products_df.alias("pr"), F.col("sd.sls_prd_key") == F.col("pr.product_number"), "left") \
    .join(dim_customers_df.alias("cu"), F.col("sd.sls_cust_id") == F.col("cu.customer_id"), "left") \
    .select(
        F.col("sd.sls_ord_num").alias("order_number"),
        F.col("pr.product_key").alias("product_key"),
        F.col("cu.customer_key").alias("customer_key"),
        F.col("sd.sls_order_dt").alias("order_date"),
        F.col("sd.sls_ship_dt").alias("shipping_date"),
        F.col("sd.sls_due_dt").alias("due_date"),
        F.col("sd.sls_sales").alias("sales_amount"),
        F.col("sd.sls_quantity").alias("quantity"),
        F.col("sd.sls_price").alias("price")
    )

fact_sales_df.createOrReplaceTempView("gold_fact_sales")
# Or save permanently:
# fact_sales_df.write.mode("overwrite").saveAsTable("gold.fact_sales")

