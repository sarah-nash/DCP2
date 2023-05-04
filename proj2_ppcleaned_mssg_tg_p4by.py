# Pro Publica cleaned (new_sample) - Word Frequency for Message, Target_Gender, Paid_for_by
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import pyspark
import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains, split
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer 
from pyspark.sql.functions import split, explode, regexp_replace, desc, lower
from pyspark.ml.feature import StopWordsRemover
from nltk.corpus import stopwords

spark = SparkSession.builder.master("local[*]").appName("project2init").getOrCreate()
# /home/madkins/data/new_sample.csv
df = spark.read.csv("/home/madkins/example/pro_publica_sample_cleaned.csv", 
				inferSchema = True,
				header = True,
				sep = ',',
				#quote = "",
				escape = '"',
				multiLine = True)

nltk.download('stopwords')
stop_words = stopwords.words('english')

stop_words = StopWordsRemover().getStopWords() + ["the", "because", "and", "to", "a", "an", "are", "as", "at", "be", "by", "for", "from", "has", "he", "she", "in", "is", "it", "its", "of", "on", "that", "to", "was", "were", "with", "like", "say", "back", "hey", "they",
"said", "one", "know", "re", "m", "get", "d", "says", "time", "eyes", "didn", "even", "going", "still"]

df_mssg = df.select("message").na.drop()  
#print(df_ads)
#df_ads.show()

# remove special characters and lower the case
df_cleaned_mssg = df.select("message").na.drop()

df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", regexp_replace(df_cleaned_mssg.message, '<[^>]+>', '')) ## added

df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", regexp_replace(df_cleaned_mssg.message, '[^a-zA-Z\\s]', ''))
df_cleaned_mssg = df_cleaned_mssg.withColumn("cleaned_text", lower(df_cleaned_mssg.cleaned_text))
#df_cleaned_ads.show()

# tokenize the text
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_mssg = tokenizer.transform(df_cleaned_mssg)
#df_tokenized_ads.show()

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_mssg = remover.transform(df_tokenized_mssg)
#df_cleaned_ads.show()

# explode the filtered words
df_words_mssg = df_cleaned_mssg.select(explode("filtered").alias("word")).filter(col("word") != '').filter(col("word") != ' ') # added. 
#df_words_mssg.show()

# count frequency of each word
df_frequency_mssg = df_words_mssg.groupBy("word").count()

# sort the words in descending order of their frequency
df_sorted_mssg = df_frequency_mssg.sort(desc("count"))

# display the top 10 most common words
print("Word Frequency - Message")
df_sorted_mssg.show(10)

df_sorted_mssg = df_sorted_mssg.limit(10)

output_path = "/home/madkins/example/ppcleaned_mssg.csv"

df_sorted_mssg.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)

# Most Common Words in the Sample 
# Example
#df_ads.show(3, truncate=False)
#df_ads.show(1)

####################################################################

df_targ = df.select("target_gender").na.drop()  
#print(df_ads)
#df_ads.show()

# remove special characters and lower the case
df_cleaned_targ = df.select("target_gender").na.drop()

df_cleaned_targ = df_cleaned_targ.withColumn("cleaned_text", regexp_replace(df_cleaned_targ.target_gender, '<[^>]+>', '')) ## added

df_cleaned_targ = df_cleaned_targ.withColumn("cleaned_text", regexp_replace(df_cleaned_targ.target_gender, '[^a-zA-Z\\s]', ''))
df_cleaned_targ = df_cleaned_targ.withColumn("cleaned_text", lower(df_cleaned_targ.cleaned_text))
#df_cleaned_ads.show()

# tokenize the text
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_targ = tokenizer.transform(df_cleaned_targ)
#df_tokenized_ads.show()

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_targ = remover.transform(df_tokenized_targ)
#df_cleaned_ads.show()

# explode the filtered words
df_words_targ = df_cleaned_targ.select(explode("filtered").alias("word")).filter(col("word") != '').filter(col("word") != ' ') # added. 
#df_words_targ.show()

# count frequency of each word
df_frequency_targ = df_words_targ.groupBy("word").count()

# sort the words in descending order of their frequency
df_sorted_targ = df_frequency_targ.sort(desc("count"))

# display the top 10 most common words
print("Word Frequency - Target_Gender")
df_sorted_targ.show(10)

df_sorted_targ = df_sorted_targ.limit(3)

output_path = "/home/madkins/example/ppcleaned_tg.csv"

df_sorted_targ.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)

################################################################

#df_paid4 = df.select("paid_for_by").na.drop()  
#print(df_ads)
#df_ads.show()
df_paid4 = df.filter(col("paid_for_by").isNotNull())

# Count instances of each company name using groupBy and count functions
paid4by_counts = df.groupBy("paid_for_by").agg(count("*").alias("count"))
sorted_paid4by_counts = paid4by_counts.orderBy(desc("count"))

# Show the results
sorted_paid4by_counts.show(10)

sorted_paid4by_counts = sorted_paid4by_counts.limit(10)

output_path = "/home/madkins/example/ppcleaned_p4by.csv"

sorted_paid4by_counts.write.format("csv") \
    .option("header", "true") \
    .option("truncate", False) \
    .mode("overwrite") \
    .save(output_path)

'''
# remove special characters and lower the case
df_cleaned_paid4 = df.select("paid_for_by").na.drop()

df_cleaned_paid4 = df_cleaned_paid4.withColumn("cleaned_text", regexp_replace(df_cleaned_paid4.paid_for_by, '<[^>]+>', '')) ## added

df_cleaned_paid4 = df_cleaned_paid4.withColumn("cleaned_text", regexp_replace(df_cleaned_paid4.paid_for_by, '[^a-zA-Z\\s]', ''))
df_cleaned_paid4 = df_cleaned_paid4.withColumn("cleaned_text", lower(df_cleaned_paid4.cleaned_text))
#df_cleaned_ads.show()

# tokenize the text
tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_paid4 = tokenizer.transform(df_cleaned_paid4)
#df_tokenized_ads.show()

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_cleaned_paid4 = remover.transform(df_tokenized_paid4)
#df_cleaned_ads.show()

# explode the filtered words
df_words_paid4 = df_cleaned_paid4.select(explode("filtered").alias("word")).filter(col("word") != '').filter(col("word") != ' ') # added. 
#df_words_paid4.show()

# count frequency of each word
df_frequency_paid4 = df_words_paid4.groupBy("word").count()

# sort the words in descending order of their frequency
df_sorted_paid4 = df_frequency_paid4.sort(desc("count"))

# display the top 10 most common words
print("Word Frequency - Paid_for_by")
df_sorted_paid4.show(10)

#df.show(10)
'''