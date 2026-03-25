"""Build a cloud-based data pipeline to:

Ingest raw supply chain data

Store it in Azure Data Lake

Clean & transform using Databricks

Visualize insights in Power BI"""
# ADLS Storage Account "
container_name = ""
storage_account_name = ""


# OAuth 2.0 Endpoint
oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

# Set Spark Config for ADLS Gen2 OAuth
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", oauth_endpoint)

print("Connection Configured Successfully")

#for aws_supply_chain_orders_raw
#reading data from data lake storage
df_supply_chain = spark.read.format("csv") \
.option("header","true") \
.load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/aws_supply_chain_orders_raw.csv")
df_supply_chain.printSchema()

#converting required columns to required data type
from pyspark.sql.functions import col

df_supply_chain = df_supply_chain.withColumn("order_qty", col("order_qty").cast("int")) \
       .withColumn("delivery_time_days", col("delivery_time_days").cast("int"))
df_supply_chain.display()

#Converting Date Columns
from pyspark.sql.functions import to_date

df_supply_chain = df_supply_chain.withColumn("order_date", to_date("order_date")) \
       .withColumn("delivery_date", to_date("delivery_date"))
df_supply_chain.display()

#Handling Missing Values
df_supply_chain = df_supply_chain.dropna(subset=["order_id", "warehouse", "region"])

df_supply_chain = df_supply_chain.fillna({
    "order_qty": 0,
    "delivery_time_days": 0
})

#Cleaning Text Columns
from pyspark.sql.functions import trim, upper

df_supply_chain = df_supply_chain.withColumn("region", upper(trim(col("region")))) \
       .withColumn("warehouse", trim(col("warehouse"))) \
       .withColumn("product", trim(col("product")))
df_supply_chain.display()

#standardizing status column
df_supply_chain=df_supply_chain.withColumn("status",upper(trim(col("status"))))
df_supply_chain.display()

#creating new column for Delivery Status
from pyspark.sql.functions import when

df_supply_chain = df_supply_chain.withColumn(
    "delivery_status",
    when(col("delivery_time_days") > 5, "Delayed")
    .otherwise("On-Time")
)

#storing data to curated
df_supply_chain.write.mode("overwrite").parquet(
f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/curated/"
)

#store in sql database
server=""
database=""
username=""
password=""
jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database}"

connection_properties = {
"user": username,
"password": password,
"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Save results to Azure SQL Database
df_supply_chain.write.jdbc(
url=jdbc_url,
table="dbo.supply_chain",
mode="overwrite",
properties=connection_properties
)


#for aws_inventory_logistics_raw
#reading data from data lake
df_inventory=spark.read.format("csv") \
.option("header","true") \
.load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/aws_inventory_logistics_raw.csv")

#Converting Numeric Columns
from pyspark.sql.functions import col

df_inventory = df_inventory.withColumn("stock_level", col("stock_level").cast("int")) \
               .withColumn("reorder_level", col("reorder_level").cast("int")) \
               .withColumn("transport_cost", col("transport_cost").cast("double"))
df_inventory.display()

#converting date column
from pyspark.sql.functions import to_date

df_inventory = df_inventory.withColumn("last_updated", to_date("last_updated"))

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
#handling missing values
df_inventory = df_inventory.dropna(subset=["inventory_id", "warehouse", "product"])

df_inventory = df_inventory.fillna({
    "stock_level": 0,
    "reorder_level": 0,
    "transport_cost": 0.0
})


#Clean Text Columns
df_inventory=df_inventory.withColumn("warehouse", trim(col("warehouse"))) \
               .withColumn("product", trim(col("product"))) \
               .withColumn("supplier", upper(trim(col("supplier"))))

#creating reorder flag
from pyspark.sql.functions import when

df_inventory = df_inventory.withColumn(
    "reorder_flag",
    when(col("stock_level") < col("reorder_level"), "Yes")
    .otherwise("No")
)

#storing data to curated
df_inventory.write.mode("overwrite").parquet(
f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/curated/inventory"
)

# Save results to Azure SQL Database
df_inventory.write.jdbc(
url=jdbc_url,
table="dbo.inventory",
mode="overwrite",
properties=connection_properties
)