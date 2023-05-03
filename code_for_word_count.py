import re
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ExtractWords").getOrCreate()

# Read the text into a DataFrame
df = spark.read.csv("one_column.csv")

# Extract the words from the text
df = df.rdd.flatMap(lambda row: re.findall(r"\w+", row)).collect()

# Remove duplicate words
df = df.dropDuplicates()