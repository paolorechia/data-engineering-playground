from pyspark.sql import DataFrame
from utils.spark_session import spark


if __name__ == "__main__":
    spark.sql("CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    data = [{"id": 1, "name": "bla", "age": 25}]
    new_data: DataFrame = spark.createDataFrame(data)
    new_data.write.mode("overwrite").format("delta").saveAsTable("student")


    data = [{"id": 5, "name": "bla", "age": 25}]
    new_data: DataFrame = spark.createDataFrame(data)
    new_data.write.mode("overwrite").format("delta").saveAsTable("student")
