import os
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

# Set the Spark version (adjust to match your installed version)
os.environ['SPARK_VERSION'] = '3.3'

# Initialize Spark session
spark = SparkSession.builder.appName('DeequExample').getOrCreate()

# Load data into Spark DataFrame
data = spark.read.csv('data.csv', header=True, inferSchema=True)

# Define data quality checks
check = Check(spark, CheckLevel.Warning, "Data Quality Check")

# Example checks: completeness, uniqueness, and range
check = check.hasSize(lambda x: x > 0) \
    .isComplete("feature1") \
    .isUnique("feature2") \
    .isContainedIn("feature3", [150, 160, 170, 180, 190, 200])  # Example valid range

# Run the checks
result = VerificationSuite(spark).onData(data).addCheck(check).run()

# Convert the result to a DataFrame and show
result_df = VerificationResult.checkResultsAsDataFrame(spark, result)
result_df.show()
