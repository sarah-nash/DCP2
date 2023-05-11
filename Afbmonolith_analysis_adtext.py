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
stop_words = StopWordsRemover().getStopWords() + ["us", "see", "get", "join", "de", "day", "today", "like",
                                                  "need", "people", "support", "year", "gift", "time", "one",
                                                  "community", "know", "make", "work", "take", "en", "help",
                                                  "learn", "every", "years", "page", "sign", "want"]

df_adtext = df.select(" ad_text").na.drop()
#df_adtext.show()
df_adtext = df_adtext.withColumn("cleaned_text", regexp_replace(df_adtext[" ad_text"], '<[^>]+>', ''))
df_adtext = df_adtext.withColumn("cleaned_text", regexp_replace(df_adtext[" ad_text"], '[^a-zA-Z\\s]', ''))
df_adtext = df_adtext.withColumn("cleaned_text", lower(df_adtext.cleaned_text))

tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_adtext = tokenizer.transform(df_adtext)

remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_adtext = remover.transform(df_tokenized_adtext)

df_words_adtext = df_adtext.select(explode("filtered").alias("word")).filter(col("word") != '')

df_freq_adtext = df_words_adtext.groupBy("word").count()

df_sorted_adtext = df_freq_adtext.sort(desc("count"))

print("Word Frequency - Ad_text")
df_sorted_adtext.show(10)
#df.printSchema()

df_sorted_adtext = df_sorted_adtext.limit(10)

output_path = "/home/madkins/example/Afbmonolith_adtext.csv"

# write the output DataFrame to a CSV file
df_sorted_adtext.write.format("csv").option("header", "true").mode("overwrite").save(output_path)
