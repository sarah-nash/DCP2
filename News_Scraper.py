from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
import requests
from wikipediaapi import Wikipedia
import re


# Initialize a SparkSession
spark = SparkSession.builder.appName('news_scraper').getOrCreate()

wiki = Wikipedia('en')


# Set the API key
clinet_id = "1aa56f0ce7164d5346208bfdf0e292bf"
client_secret = "c288490c0816bfe3c2c9af1377fdc8c57572b289"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxYWE1NmYwY2U3MTY0ZDUzNDYyMDhiZmRmMGUyOTJiZiIsImp0aSI6IjZiYWVjYjM0ZjYxNzM2ZTFmYTdmYzMzZjg0ZGMxMzZhZWU0ZDA3YTY3NTEyNGViZDdiOTdmMDIzNTI4MWU0ZTA3ZWVmOTA4MzhlNmVkNzQ2IiwiaWF0IjoxNjgzOTE5NDUzLjU1ODY0NiwibmJmIjoxNjgzOTE5NDUzLjU1ODY0OSwiZXhwIjozMzI0MDgyODI1My41NTY1MjYsInN1YiI6IjcyNzk5NTM3IiwiaXNzIjoiaHR0cHM6Ly9tZXRhLndpa2ltZWRpYS5vcmciLCJyYXRlbGltaXQiOnsicmVxdWVzdHNfcGVyX3VuaXQiOjUwMDAsInVuaXQiOiJIT1VSIn0sInNjb3BlcyI6WyJiYXNpYyJdfQ.rmbxk0g8Snuov4Y3jYqbZ3DjzZSZQBdHrS8GSBN_fN7IVMI2UBKGOYeBqCx0ZlbRvZpa05vQ2q4M-htffSLMvZK7hs1rlXCHFN_QbT_aADM82pANj9F4LjBe3MVhC-EwNubBhA7-DMAQyvEryJ0n8UTIMwXLVNkpgfIwkUVJbro4beYy1bMU97XCAeIfVBjonK9n-yzttMIaNQF5kLJ4AcWmpm6H3ElumpAVIQRrqTTVdhEY6sukp2Biw7Z4u18iRR18FWTbMojqTsUidh2xWHkO2NSNE0N5FjNvIk-A0m73lXB2F50bGe9WHzYyZrHFfvrimf76PKAmq4uhm8txBjwXgMH8DwsHgacGkBpwNpWFtdWC5Qbl1M4FN4bCxX01j4WsM9LzezCbAe9lKQRBrvza3Wheyh09evdWQFWVy6kz3XsuB4gZf0iHuRHszYgfO0EqVr3CWln0vO_vaIO8WXlYOPA07pay6ZPdeHEt6LtzTRPJ2R4iiBt9QdNelVhW5o_RWJXa8NmeScD9-_AIteA7J7AIv6T9oUsJbqYB0NXTowDRoQJ8L4E31x31W2iBwKtgykYo0MuvB2SQCkTd5P6cVE36aRxubbLdkOsNmXWHfR2fwPdWcil9ZJBP8giwRNYkMs_-dnyd0fdy8F1No4hHLhX2m_K9rrFECMuglFE"
wiki_api = "https://en.wikipedia.org/w/api.php"
# Set the headers
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {}'.format(access_token)
}

# List of URLs to scrape
urls = ['https://en.wikipedia.org/wiki/Portal:Current_events/September_2017',
        'https://en.wikipedia.org/wiki/Portal:Current_events/October_2017',
        'https://en.wikipedia.org/wiki/Portal:Current_events/November_2017',
        'https://en.wikipedia.org/wiki/Portal:Current_events/December_2017',
        'https://en.wikipedia.org/wiki/Portal:Current_events/January_2018',
        'https://en.wikipedia.org/wiki/Portal:Current_events/February_2018',
        'https://en.wikipedia.org/wiki/Portal:Current_events/March_2018',
        'https://en.wikipedia.org/wiki/Portal:Current_events/April_2018',
        'https://en.wikipedia.org/wiki/Portal:Current_events/December_2020',
        'https://en.wikipedia.org/wiki/Portal:Current_events/January_2021',
        'https://en.wikipedia.org/wiki/Portal:Current_events/February_2021',
        'https://en.wikipedia.org/wiki/Portal:Current_events/March_2021']

# Scrape the webpages
news = []
for url in urls:
    html = requests.get(url).content
    soup = BeautifulSoup(html, 'html.parser')

    # Extract the news 
    for item in soup.select('.vevent'):
        date = item.select_one('.summary').text
        
        # Extract the title for each article for each day
        for title_link in item.select('.description'):
            title = title_link.text
            sub_desc = ''
            sub_desc_links = []
            sub_desc_links_str = ''
            
            # Check for sub-descriptions and links within the main description
            for sub_desc_link in title_link.select('.sub-description'):
                sub_desc = sub_desc + sub_desc_link.text.strip() + '\n'
                sub_desc_links.append(sub_desc_link.find('a'))
            
            # Remove sub-descriptions from the main description
            for sub_desc_link in title_link.select('.sub-description'):
                sub_desc_link.decompose()
            
            # Extract the links that are in each title
            for link in title_link.select('a'):
                link = link['href']
                if not link.startswith('/wiki/'):
                    sub_desc_links_str = sub_desc_links_str + link + '\n'
                
            news.append((date, title.strip(), sub_desc.strip(), sub_desc_links_str.strip()))

# Create a PySpark DataFrame
news_df = spark.createDataFrame(news, ['date', 'title', 'link'])
# Filter out titles containing 'edit', 'history', or 'watch'
news_df = news_df.filter(~news_df.title.contains('edit') & 
                         ~news_df.title.contains('history') & 
                         ~news_df.title.contains('watch'))

# Remove the link column
news_df = news_df.drop('link')

# Rename _4 column to link
news_df = news_df.withColumnRenamed('_4', 'link')

# Show the DataFrame
news_df.show()

# Save every csv into 1 csv file
news_df.coalesce(1).write.csv('all_news.csv2.0',  header=True, sep=',', escape='"', ignoreLeadingWhiteSpace=True)
