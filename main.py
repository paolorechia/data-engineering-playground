from pyspark.sql import DataFrame
from utils.spark_session import spark


if __name__ == "__main__":
    # Uncomment to create table the first time
    # Hive metastore throws exception, but we can ignore it (end result is displayed)
    # spark.sql("CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    
    data = [
        {"id": 30, "name": "blerg", "age": 32},
        {"id": 27, "name": "bla2", "age": 15},
        {"id": 1, "name": "bla1", "age": 27}
    ]
    new_data: DataFrame = spark.createDataFrame(data)
    new_data.write.mode("overwrite").format("delta").saveAsTable("student")

    spark.sql("SELECT * FROM student").show()