from utils.spark_session import spark

results = spark.sql('select * from table_changes("student", 0);')
results.show()