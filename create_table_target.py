from utils.spark_session import spark

students = spark.sql("SELECT * FROM student")
students.write.mode("error").format("delta").saveAsTable("golden_student")
