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

# reading data from blob storage

df = spark.read.format("csv") \
.option("header","true") \
.load(f"{Loaction}")
display(df)
#selecting required columns to train model
selected_df = df.select(
"Purchase_Frequency",
"Browsing_Frequency",
"Cart_Completion_Frequency",
"Cart_Abandonment_Factors",
"Customer_Reviews_Importance",
"Recommendation_Helpfulness",
"Shopping_Satisfaction"
)

display(selected_df)

#cleaning data
clean_df = selected_df.dropna()
display(clean_df)

#feature engineering
#Converting Text Columns to Numeric
from pyspark.ml.feature import StringIndexer

indexer1 = StringIndexer(inputCol="Purchase_Frequency", outputCol="purchase_freq_index")
indexer2 = StringIndexer(inputCol="Browsing_Frequency", outputCol="browsing_freq_index")
indexer3 = StringIndexer(inputCol="Cart_Completion_Frequency", outputCol="cart_completion_index")

df_indexed = indexer1.fit(clean_df).transform(clean_df)
df_indexed = indexer2.fit(df_indexed).transform(df_indexed)
df_indexed = indexer3.fit(df_indexed).transform(df_indexed)

display(df_indexed)

#creating feature vector
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
inputCols=[
"purchase_freq_index",
"browsing_freq_index",
"cart_completion_index"
],
outputCol="features"
)

feature_df = assembler.transform(df_indexed)

display(feature_df)

#Applying kmeans model
from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=3, seed=1)

model = kmeans.fit(feature_df)

clusters = model.transform(feature_df)

display(clusters)
clusters.printSchema()

#storing data to curated
clusters.write.mode("overwrite").parquet(
"{Loacation}"
)

from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array

customerSegment_df = clusters.select(
    col("Purchase_Frequency"),
    col("Browsing_Frequency"),
    col("Cart_Completion_Frequency"),
    col("Cart_Abandonment_Factors"),
    col("Customer_Reviews_Importance"),
    col("Recommendation_Helpfulness"),
    col("Shopping_Satisfaction"),
    col("purchase_freq_index"),
    col("browsing_freq_index"),
    col("cart_completion_index"),
    vector_to_array(col("features"))[0].alias("feature"),
    col("prediction").alias("segment")
)

#connecting to azure sql server database
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

# Save results to Azure SQL Database
customerSegment_df.write.jdbc(
url=jdbc_url,
table="dbo.CustomerSegments",
mode="overwrite",
properties=connection_properties
)
