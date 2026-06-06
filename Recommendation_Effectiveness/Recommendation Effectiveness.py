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

# Loading data from blob storage

df = spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/processed/Amazon Customer Behavior Survey.csv")
display(df)

# selecting required columns
from pyspark.sql.functions import col

rec_df = df.select(
"Personalized_Recommendation_Frequency",
"Recommendation_Helpfulness",
"Shopping_Satisfaction",
col("`Rating_Accuracy `")
)

display(rec_df)

# cleaning data such as removing nulls

from pyspark.sql.functions import col

rec_df = rec_df.filter(
col("Personalized_Recommendation_Frequency").isNotNull() &
col("Recommendation_Helpfulness").isNotNull()
)
rec_df.display()

#Feature Engineering
#Convert text responses into numeric scores.
#Frequency Mapping
from pyspark.sql.functions import when

rec_df = rec_df.withColumn(
"freq_score",
when(rec_df.Personalized_Recommendation_Frequency=="No",0)
.when(rec_df.Personalized_Recommendation_Frequency=="Sometimes",1)
.when(rec_df.Personalized_Recommendation_Frequency=="Yes",2)

)
rec_df.display()

#Create helpfulness score
#Helpfulness Mapping
rec_df = rec_df.withColumn(
"helpfulness_score",
when(rec_df.Recommendation_Helpfulness=="No",0)
.when(rec_df.Recommendation_Helpfulness=="Sometimes",1)
.when(rec_df.Recommendation_Helpfulness=="Yes",2)
)
rec_df.display()

#Calculate Recommendation Metrics
avg_helpfulness = rec_df.groupBy("recommendation_freq_score") \
.avg("helpfulness_score")
avg_helpfulness.display()

#Satisfaction Impact
satisfaction_analysis = rec_df.groupBy("recommendation_freq_score") \
.count()
satisfaction_analysis.display()

#Create Metrics
#Average Helpfulness per Frequency
helpfulness_df = rec_df.groupBy("freq_score") \
.avg("helpfulness_score")
helpfulness_df.display()

#Satisfaction Impact
satisfaction_df = rec_df.groupBy("freq_score") \
.avg("Shopping_Satisfaction")
satisfaction_df.display()

#Combine metrics.
from pyspark.sql.functions import col

result_df = helpfulness_df.join(
satisfaction_df, "freq_score"
).select(
col("freq_score"),
col("avg(helpfulness_score)").alias("avg_helpfulness"),
col("avg(Shopping_Satisfaction)").alias("avg_satisfaction")
)

display(result_df)

# connecting to azure sql server

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
result_df.write.jdbc(
url=jdbc_url,
table="dbo.RecommendationEffectiveness",
mode="overwrite",
properties=connection_properties
)
