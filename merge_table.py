"""This is a somewhat convoluted example of merge with CDC.

In reality, all rows need to be checked in this example, however in some cases,
we could build tables where checking for the 'table_changes' would help more.
"""
from delta.tables import DeltaTable
from utils.spark_session import spark

# Change this date to set up how far in the past should look for changes.
today = '2024-07-24 18:54:00'


print(f"All changes since {today}")
results = spark.sql(f'select distinct age, id, name from table_changes("student", "{today}");')
results.show()

print("Students table")
students = spark.sql("SELECT * FROM student")
students.show()

print("Students joined")
joined = students.join(results, on=["age", "id", "name"], how="inner")
joined.show()

print("Target table pre merge")
golden_student = spark.sql("select * from golden_student")
golden_student.show()

golden_delta = DeltaTable.forName(spark, "golden_student")

golden_delta.alias("golden").merge(
    joined.alias("silver"),
    "golden.id = silver.id"
).whenMatchedUpdate(
    set = {
        "id": "silver.id",
        "age": "silver.age",
        "name": "silver.name"
    }
).whenNotMatchedInsert(
    values = {
        "id": "silver.id",
        "age": "silver.age",
        "name": "silver.name"
    }
).execute()


print("Gold table after merge")
spark.sql("select * from golden_student").show()