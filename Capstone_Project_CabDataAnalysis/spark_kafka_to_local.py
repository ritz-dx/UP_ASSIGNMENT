from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

spark = SparkSession.builder.appName('clickstream').master('local[1]').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
  .option("subscribe", "de-capstone3") \
  .option("auto.offset.reset", "earliest") \
  .option("startingOffsets", "earliest") \
  .load()

schema = StructType([
    StructField("customer_id", StringType(), True), 
    StructField("app_version", StringType(), True), 
    StructField("OS_version", StringType(), True), 
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("page_id", StringType(), True), 
    StructField("button_id", StringType(), True), 
    StructField("is_button_click", StringType(), True), 
    StructField("is_page_view", StringType(), True), 
    StructField("is_scroll_up", StringType(), True), 
    StructField("is_scroll_down", StringType(), True),
    StructField("timestamp\n", StringType(), True)
])

df = df.selectExpr('CAST(value AS STRING)') \
    .select(from_json('value', schema).alias("value" )) \
    .select("value.*")

timeAndCountryKPIQuery = df.writeStream \
   .format("json") \
   .outputMode("append") \
   .option("truncate", "false") \
   .option("path", "clickstream-data") \
   .option("checkpointLocation", "clickstream-data-cp") \
   .trigger(processingTime="1 minute") \
   .start() \
  .awaitTermination()