import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_CHATBOT_2025_03_14', 'S3_CHATBOT_2025_03_29', 'S3_CHATBOT_2025_04_18', 'S3_WEATHER_2025_03_14', 'S3_WEATHER_2025_03_29','S3_WEATHER_2025_04_18', 'DESTINATION', 'PASSWORD', 'JDBC_URL', 'USER'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Reading all the csv files and convert them into dataframe
chatbot_2025_03_14 = spark.read.format("csv").option("header", "true").load(args["S3_CHATBOT_2025_03_14"])
weather_2025_03_14 = spark.read.format("csv").option("header", "true").load(args["S3_WEATHER_2025_03_14"])
chatbot_2025_03_29 = spark.read.format("csv").option("header", "true").load(args["S3_CHATBOT_2025_03_29"])
weather_2025_03_29 = spark.read.format("csv").option("header", "true").load(args["S3_WEATHER_2025_03_29"])
chatbot_2025_04_18 = spark.read.format("csv").option("header", "true").load(args["S3_CHATBOT_2025_04_18"])
weather_2025_04_18 = spark.read.format("csv").option("header", "true").load(args["S3_WEATHER_2025_04_18"])

# combine them according to their dates
df_2025_03_14 = chatbot_2025_03_14.join(weather_2025_03_14, chatbot_2025_03_14["user_location"] == weather_2025_03_14["Location"], "inner")
df_2025_03_29 = chatbot_2025_03_29.join(weather_2025_03_29, chatbot_2025_03_29["user_location"] == weather_2025_03_29["Location"], "inner")
df_2025_04_18 = chatbot_2025_04_18.join(weather_2025_04_18, chatbot_2025_04_18["user_location"] == weather_2025_04_18["Location"], "inner")

# combine them all into unified view
df_2025_03 = df_2025_03_14.unionByName(df_2025_03_29)
df = df_2025_03.unionByName(df_2025_04_18)

# Now drop the "Location" column as it same as "user_location"
df = df.drop("Location")

# change the datatype of timestamp
df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Now create a Fact table
fact_table = df.select("session_id", "timestamp", "sentiment", "weather")

# Two source table that provides information to fact table
users_dim = df.select("session_id", "user_id", "user_email", "user_location")
comments_dim = df.select("session_id", "message_count", "conversation_summary")

# Now save the transforms data into RDS
url = args["JDBC_URL"]
connection_properties = {
    "user" : args["USER"],
    "password" : args["PASSWORD"],
    "driver" : "org.postgresql.Driver"
}

fact_table.write.jdbc(url=url, table="fact_sentiment", mode="append", properties=connection_properties)
users_dim.write.jdbc(url=url, table="users_dim", mode="append", properties=connection_properties)
comments_dim.write.jdbc(url=url, table="comments_dim", mode="append", properties=connection_properties)

job.commit()