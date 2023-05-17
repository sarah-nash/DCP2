import pyspark as spark
import re
import csv 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lag, col, udf

# all_news = Scraped data with sentiment
# Fb_Mono = FBmonolith with sentiment
# Fb_Pac = PBpac with sentiment

# Do an analysis on the sentiment for all_news
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

# Do an analysis on the sentiment for Fb_Mono
Fb_Mono = spark.read.csv("/home/rblaha/FBmonolith_With_Sentiment.csv", ignoreLeadingWhiteSpace=True, header=True, inferSchema=True, multiLine=True, sep=',', escape='"')

# aggerate the data by month and find the average sentiment for each month
Fb_Mono = Fb_Mono.withColumn("observed_at", F.to_date(F.col("observed_at"), "yyyy-MM-dd"))

# Group the data by year and month of observed_at, and calculate the average sentiment for each group
Fb_Mono = Fb_Mono.groupBy(F.year("observed_at").alias("year"), F.month("observed_at").alias("month")) \
              .agg(F.avg("sentiment").alias("sentiment_avg"))
              
# Sort the data by year and month
Fb_Mono = Fb_Mono.orderBy("year", "month")

# Read in Fb_Pac data
Fb_Pac = spark.read.csv("/home/rblaha/FBpac_With_Sentiment.csv", ignoreLeadingWhiteSpace=True, header=True, inferSchema=True, multiLine=True, sep=',', escape='"')

# aggerate the data by month and find the average sentiment for each month
Fb_Pac = Fb_Pac.withColumn("created_at", F.to_date(F.col("created_at"), "yyyy-MM-dd"))

# Group the data by year and month of observed_at, and calculate the average sentiment for each group
Fb_Pac = Fb_Pac.groupBy(F.year("created_at").alias("year"), F.month("created_at").alias("month")) \
                .agg(F.avg("sentiment").alias("sentiment_avg"))
                
# Sort the data by year and month
Fb_Pac = Fb_Pac.orderBy("year", "month")

# Show the data
all_news.show(10)
Fb_Mono.show(10)
Fb_Pac.show(10)


# Join to create two data frames. One with All_News and Fb_Mono, and the other with All_News and Fb_Pac
all_news_mono = all_news.join(Fb_Mono, ["year", "month"], "inner")
all_news_pac = all_news.join(Fb_Pac, ["year", "month"], "inner")




# Show the new data frame
all_news_mono.show(10)
all_news_pac.show(10)


# Do a linear regression on the data frames
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Create the vector assembler for the features
assembler = VectorAssembler(inputCols=["year", "month"], outputCol="features")

# Transform the data
all_news_mono = assembler.transform(all_news_mono)
all_news_pac = assembler.transform(all_news_pac)

# Create a multi-linear regression object
lr = LinearRegression(featuresCol="features", labelCol="real_world_avg_sentiment")

# Fit the model to the data
lrModel_mono = lr.fit(all_news_mono)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel_mono.coefficients))
print("Intercept: %s" % str(lrModel_mono.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary_mono = lrModel_mono.summary
print("numIterations: %d" % trainingSummary_mono.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary_mono.objectiveHistory))
trainingSummary_mono.residuals.show()
print("RMSE: %f" % trainingSummary_mono.rootMeanSquaredError)
print("r2: %f" % trainingSummary_mono.r2)

# Fit the model to the data
lrModel_pac = lr.fit(all_news_pac)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel_pac.coefficients))
print("Intercept: %s" % str(lrModel_pac.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary_pac = lrModel_pac.summary
print("numIterations: %d" % trainingSummary_pac.totalIterations)

print("objectiveHistory: %s" % str(trainingSummary_pac.objectiveHistory))
trainingSummary_pac.residuals.show()

print("RMSE: %f" % trainingSummary_pac.rootMeanSquaredError)
print("r2: %f" % trainingSummary_pac.r2)

# Create a new data frame with the predictions
predictions_mono = lrModel_mono.transform(all_news_mono)
predictions_pac = lrModel_pac.transform(all_news_pac)

# Show the predictions
predictions_mono.show(10)
predictions_pac.show(10)



