library(dplyr)
library(ggplot2)
library(lubridate)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

##########################  Recommended Objects in this File #############################
#         ****   You can call any of these by highlighting one, then Shift + Enter   ****

# mono_avg_sentiment_line  and  pb_avg_sentiment_line - Sponsor Sentiment distributions
# pb_avg_sponsor_sentiment  - Sponsor barplot for 2017-2019

##########################################################################################



# Data Loading
mono_sentiment_df <- read.csv("fb_monolith_cleaner.csv")
View(sentiment_df)

mono_sentiment_df$observed_at <-
  as.Date(mono_sentiment_df$observed_at, "%m/%d/%Y")

# Data PreProcessing
mono_sentiment_df_grouped <- mono_sentiment_df %>%
  group_by(observed_at) %>%
  summarise(avg = mean(sentiment))

mono_avg_sentiment_line <-
  ggplot(mono_sentiment_df_grouped, aes(x = observed_at, y = avg)) +
  geom_line() +
  labs(title = "Daily Average Sentiment from 2020 - 2021 According to FbMonolith",
       x = "Date",
       y = "Avergae Sentiment") +
  scale_x_date(date_labels = "%d %b-%Y",
               date_breaks = "1 weeks",
               breaks = waiver())


# Data Loading
pb_sentiment_df <- read.csv("FBpac_With_Sentiment.csv")
View(pb_sponsor_sentiment)

pb_sentiment_df$created_at <-
  as.Date(pb_sentiment_df$created_at, "%m/%d/%Y")
pb_sentiment_df$sentiment <- as.numeric(pb_sentiment_df$sentiment)

# Data PreProcessing
pb_sentiment_df_grouped <- pb_sentiment_df %>%
  group_by(created_at) %>%
  summarise(avg = mean(sentiment))

pb_avg_sentiment_line <-
  ggplot(pb_sentiment_df_grouped, aes(x = created_at, y = avg)) +
  geom_line() +
  labs(title = "Daily Average Sentiment from 2017 - 2019 According to Propublica",
       x = "Date",
       y = "Average Sentiment") +
  scale_x_date(date_labels = "%b-%Y",
               date_breaks = "2 months",
               breaks = waiver())

pb_sponsor_sentiment <- pb_sentiment_df %>%
  filter(title == pb_top20_pages$title) %>%
  group_by(title) %>%
  summarise(avg = mean(sentiment))

pb_avg_sponsor_sentiment <-
  ggplot(pb_sponsor_sentiment, aes(x = title, y = avg, fill = title)) +
  geom_col() +
  coord_flip() +
  theme(legend.position = "none") +
  labs(title = "Average Sentiment of Ads by Top 20 Sponsors from 2017 - 2019",
       y = "Average Sentiment",
       x = "Sponsor")
