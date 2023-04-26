import pyspark as spark
import re
from textblob import TextBlob
import json
import csv 
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (CountVectorizer, StopWordsRemover, StringIndexer,
                                Tokenizer, Word2Vec)
from pyspark.sql.functions import col, regexp_extract, when, udf, col
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


spark = spark.sql.SparkSession.builder.appName("Sentiment").getOrCreate()

# Define schema for the data
schema = StructType([
    StructField("ad_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("political_value", DoubleType(), True),
    StructField("paid_for_by", StringType(), True),
    StructField("ad_text", StringType(), True),
    StructField("observed_at", StringType(), True),
    StructField("call_to_action", StringType(), True),
    StructField("targetings", StringType(), True),
])


# Define the function to create sentiment
def get_sentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return 1.0
    else:
        return 0.0
    

# Define a UDF for the sentiment extraction function
get_sentiment_udf = udf(get_sentiment, DoubleType())

# Load Data
data = spark.read.csv("/home/rblaha/fb_monolith.csv", header=True, schema=schema, ignoreLeadingWhiteSpace=True) # Update to your path. 

# Drop any rows with missing data
data = data.na.drop()


# Cast the label column to IntegerType
data = data.withColumn("sentiment", get_sentiment_udf(col("ad_text")))

# Cast the label column to IntegerType
data = data.withColumn("label", col("sentiment"))

# convert the label to integer
data = data.withColumn("label", data["label"].cast(IntegerType()))


# Rename the existing column to "original_text"
data = data.withColumnRenamed("ad_text", "original_text")

# Tokenize the text
tokenizer = Tokenizer(inputCol="original_text", outputCol="words")

# Remove stop words
stop_words_remover = StopWordsRemover(inputCol="words")


# Create a bag of words
word2vec = Word2Vec(inputCol="words", outputCol="features")

# Create a pipeline
pipeline = Pipeline(stages=[tokenizer, stop_words_remover, word2vec])

# Fit and transform the data
pipeline_fit = pipeline.fit(data)

# Transform the data
data = pipeline_fit.transform(data)

# Split the data into training and test sets
(training_data, test_data) = data.randomSplit([0.8, 0.2], seed=100)

# Create a SVM model
SVM = LinearSVC(maxIter=80, regParam=0.1, featuresCol="features", labelCol="label", predictionCol="prediction", tol=1e-6)

# Train the model
SVM_model = SVM.fit(training_data)

# Make predictions
predictions = SVM_model.transform(test_data)

# Print the predictions with the original text and features columns
predictions.select("prediction", "label", "original_text", "features").show(10)



# Evaluate the model
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Accuracy of model at predicting sentiment was: %f" % accuracy)


# Print the coefficients and intercept for linear svc
print("Coefficients: " + str(SVM_model.coefficients))
print("Intercept: " + str(SVM_model.intercept))

# Save the model and overwrite if exists
SVM_model.write().overwrite().save("SVM_model")

# Save the pipeline and overwrite if exists
pipeline_fit.write().overwrite().save("pipeline_fit")



