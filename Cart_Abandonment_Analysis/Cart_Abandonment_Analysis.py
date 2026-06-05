'''
Event Hubs ingests real-time cart abandonment events.
Databricks Structured Streaming processes reasons (high shipping cost, better price elsewhere, changed mind).
Aggregated abandonment factors stored in dbo.CartAbandonmentAnalysis in Azure SQL.
Power BI Desktop dashboard highlights top abandonment drivers and trends.
'''
# Databricks notebook source
# connection string
connection_string="{connection string}"
eventhubConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        connection_string
    )
}

#Receive Events
stream_df = spark.readStream \
.format("eventhubs") \
.options(**eventhubConf) \
.load()

#converting event body
events = stream_df.selectExpr("CAST(body AS STRING)")

#Creating schema
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

schema = StructType() \
.add("purchase_frequency", StringType()) \
.add("browsing_frequency", StringType()) \
.add("cart_completion", StringType()) \
.add("abandonment_reason", StringType()) \
.add("satisfaction", StringType())


parsed_df = events.select(
    from_json(col("body"), schema).alias("data")
).select("data.*")

#Analyze Cart Abandonment Reasons
analysis_df = parsed_df.groupBy(
"abandonment_reason"
).count()
display(analysis_df, checkpointLocation="{Location}")

# writng streaming data
query = analysis_df.writeStream \
.outputMode("complete") \
.format("console") \
.option("checkpointLocation", "Location") \
.start()

# connecting azure sql database
server="{server-name}"
database="{db-name}"
username="{username}"
password="{password}"
jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database}"

connection_properties = {
"user": username,
"password": password,
"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# storing data to database
def write_to_sql(batch_df):
    batch_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.CartAbandonmentAnalysis") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .save()

analysis_df.writeStream \
.outputMode("complete") \
.foreachBatch(write_to_sql) \
.option("checkpointLocation", "{location}") \
.start()

