import pyspark as spark
import re
import csv 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lag, col, udf

# all_news = Scraped data with sentiment
# Fb_Mono = FBmonolith with sentiment
# Fb_Pac = PBpac with sentiment

# Do an analysis on the sentiment of the ad text over time. 
spark = spark.sql.SparkSession.builder.appName("Time_Series_Sentiment").getOrCreate()

# Load Data
all_news = spark.read.csv("/home/rblaha/All_News_With_Sentiment.csv", ignoreLeadingWhiteSpace=True, header=True, inferSchema=True, multiLine=True, sep=',', escape='"') 

# aggerate the data by month and find the average sentiment for each month
all_news = all_news.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

# Group the data by year and month of observed_at, and calculate the average sentiment for each group
all_news = all_news.groupBy(F.year("date").alias("year"), F.month("date").alias("month")) \
           .agg(F.avg("real_world_sentiment").alias("real_world_avg_sentiment"))

# Sort the data by year and month
all_news = all_news.orderBy("year", "month")


# Show the data
all_news.show(10)

