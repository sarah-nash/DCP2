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

df_obsvdat = df.select(" observed_at").na.drop()
df_obsvdat.show(3, truncate = False)
df_obsvdat = df_obsvdat.withColumn("cleaned_text", regexp_replace(df_obsvdat[" observed_at"], '<[^>]+>', ''))
df_obsvdat = df_obsvdat.withColumn("cleaned_text", regexp_replace(df_obsvdat[" observed_at"], '[^a-zA-Z\\s]', ''))
df_obsvdat = df_obsvdat.withColumn("cleaned_text", lower(df_obsvdat.cleaned_text))

tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
df_tokenized_obsvdat = tokenizer.transform(df_obsvdat)

remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=stop_words)
df_obsvdat = remover.transform(df_tokenized_obsvdat)

df_words_obsvdat = df_obsvdat.select(explode("filtered").alias("word")).filter(col("word") != '')

df_freq_obsvdat = df_words_obsvdat.groupBy("word").count()

df_sorted_obsvdat = df_freq_obsvdat.sort(desc("count"))

print("Word Frequency - observed_at")
df_sorted_obsvdat.show(10, truncate = False)
#df.printSchema()

df_sorted_obsvdat = df_sorted_obsvdat.limit(10)

output_path = "/home/madkins/example/Afbmonolith_obsvdat.csv"

# write the output DataFrame to a CSV file
df_sorted_obsvdat.write.format("csv").option("header", "true").mode("overwrite").save(output_path)
