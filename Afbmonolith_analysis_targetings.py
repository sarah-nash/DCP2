'''
Word Count Analysis (Top 10)

Dataset | Column 
----------------+
FBMON   | Targetings

This script was applied to the fbmonolith dataset in the teamnorth folder. In my analysis, I found that targetings column
appears to be a list or dictionary like data structure. If we can break targetings into separate columns, I can apply word count 
to each of those sub columns to get a better analysis of most common words. 

'''
import pyspark
import nltk
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_replace, desc, lower
from pyspark.ml.feature import StopWordsRemover, Tokenizer

spark = SparkSession.builder.master("local[*]").appName("proj2tn").getOrCreate()
df = spark.read.csv("/home/madkins/teamnorth/data/fb_monolith.csv",
				inferSchema = True,
				header = True,
				sep = ",",
				quote = "",
				escape = "",
				multiLine = True)

#df.show(1)

nltk.download('stopwords')
stop_words = stopwords.words('english')
stop_words = StopWordsRemover().getStopWords() + ["location", "locale"]
df.printSchema()
df_targetings = df.select(" targetings").na.drop()
df_targetings.show(3, truncate = False)
#df_targetings.show()
df_targetings = df_targetings.withColumn("cleaned_text", regexp_replace(df_targetings[" targetings"], '<[^>]+>', ''))
df_targetings = df_targetings.withColumn("cleaned_text", regexp_replace(df_targetings[" targetings"], '[^a-zA-Z\\s]', ''))
df_targetings = df_targetings.withColumn("cleaned_text", lower(df_targetings.cleaned_text))

tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_targetings = tokenizer.transform(df_targetings)

remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_targetings = remover.transform(df_tokenized_targetings)

df_words_targetings = df_targetings.select(explode("filtered").alias("word")).filter(col("word") != '')

df_freq_targetings = df_words_targetings.groupBy("word").count()

df_sorted_targetings = df_freq_targetings.sort(desc("count"))

print("Word Frequency - targetings")
df_sorted_targetings.show(10)
#df.printSchema()

df_sorted_targetings = df_sorted_targetings.limit(10)

output_path = "/home/madkins/example/Afbmonolith_targetings.csv"

# write the output DataFrame to a CSV file
df_sorted_targetings.write.format("csv").option("header", "true").mode("overwrite").save(output_path)
