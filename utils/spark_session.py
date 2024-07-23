from pyspark.sql import SparkSession

import delta

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()