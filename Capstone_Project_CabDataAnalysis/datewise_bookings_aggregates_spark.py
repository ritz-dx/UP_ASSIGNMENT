from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

spark = SparkSession.builder.appName('bookings').master('local[1]').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.read \
  .format("jdbc") \
  .option("url", "jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase") \
  .option("driver", "com.mysql.jdbc.Driver") \
  .option("dbtable", "bookings") \
  .option("user", "student") \
  .option("password", "STUDENT123") \
  .load()

df = df \
    .groupBy(to_date(df.pickup_timestamp,"dd-MM-yyyy").alias("booking date")) \
    .agg(count("booking_id").alias("Total Bookings")) \
    .orderBy("booking date")

df.show()

df.repartition(1).write.format("csv").save("bookings")
