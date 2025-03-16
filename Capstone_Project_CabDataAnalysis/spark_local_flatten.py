    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark.sql import functions as F
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json

    spark = SparkSession.builder.appName('clickstream').master('local[1]').getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    df = spark.read.json('clickstream-data')

    df.repartition(1).write.format("csv").save("clickstream")