from utils.spark_session import spark

# Change this date to set up how far in the past should look for changes.
today = '2024-07-24 18:54:00'

results = spark.sql(f'select * from table_changes("student", "{today}");')
results.show()


students = spark.sql("SELECT * FROM student")
students.join(results, on=["age", "id", "name"], how="inner").show()