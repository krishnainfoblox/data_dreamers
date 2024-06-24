from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

# Spark session setup (assuming a Spark cluster is available)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DeequExample').getOrCreate()

# Load data into Spark DataFrame
data = spark.read.csv('data.csv', header=True, inferSchema=True)

# Define data quality checks
check = Check(spark, CheckLevel.Warning, "Data Quality Check")

# Example checks: completeness, uniqueness, and range
check = check.hasSize(lambda x: x > 0) \
    .isComplete("column_name") \
    .isUnique("column_name") \
    .isContainedIn("column_name", ["valid_value1", "valid_value2"])

# Run the checks
result = VerificationSuite(spark).onData(data).addCheck(check).run()
result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
result_df.show()
