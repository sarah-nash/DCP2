import pyspark as spark
import re
import csv 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lag, col, udf

# Do an analysis on the sentiment of the ad text over time. 
spark = spark.sql.SparkSession.builder.appName("Time_Series_Sentiment").getOrCreate()

# Load Data
data = spark.read.csv("/home/rblaha/fbpac_clean1.csv", ignoreLeadingWhiteSpace=True, header=True, inferSchema=True, multiLine=True, sep=',', escape='"') 

# aggerate the data by month and find the average sentiment for each month
data = data.withColumn("created_at", F.to_date(F.col("created_at"), "yyyy-MM-dd"))

# Group the data by year and month of observed_at, and calculate the average sentiment for each group
data = data.groupBy(F.year("created_at").alias("year"), F.month("created_at").alias("month")) \
           .agg(F.avg("sentiment").alias("avg_sentiment"))

# Sort the data by year and month
data = data.orderBy("year", "month")

# Show the data
data.show(10)