import pyspark as spark
from textblob import TextBlob
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType

# Create SparkSession
spark = spark.sql.SparkSession.builder.appName("Rename_and_Drop_Columns").getOrCreate()

# Define the function to create sentiment
def get_sentiment(text):
    if text:
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity
        if sentiment > 0:
            return 1.0
        else:
            return 0.0
    else:
        return None

# Define a UDF for the sentiment extraction function
get_sentiment_udf = udf(get_sentiment, DoubleType())

# Read in multiline csv file
data = spark.read.csv("/home/rblaha/fb_monolith.csv", header=True, inferSchema=True, multiLine=True, sep=',', escape='"', ignoreLeadingWhiteSpace=True) # Change your path

# Filter out any rows where ad_text is null
data = data.filter(col("ad_text").isNotNull())

# Add a column for sentiment
data = data.withColumn("sentiment", get_sentiment_udf("ad_text"))

# Extract the date part from Observed_At column
data = data.withColumn("observed_at", date_format(col("observed_at"), "yyyy-MM-dd"))

# Drop the columns that are not needed
data = data.drop("page_name", "paid_for_by", "call_to_action", "targetings", " ")

# Save the data to a new csv file
data.write.csv("/home/rblaha/fb_monolith_clean3.csv", header=True, sep=',', escape='"', ignoreLeadingWhiteSpace=True) # Change your path

# Show the data
data.show(100)

