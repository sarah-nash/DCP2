library(dplyr)
library(ggplot2)
library(lubridate)
library(wordcloud2)
library(wordcloud)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

##########################  Recommended Objects in this File #############################
#         ****   You can call any of these by highlighting one, then Shift + Enter   ****

# mono_ads_per_month_year and pb_ads_per_month_year - Monthly ad counts with year indicators
# mono_ads_per_day_histo and pb_ads_per_day_histo - Daily Ad Counts
# mono_ads_per_day_density and pb_ads_per_day_density - Daily Ad Densities 

################## Counts Histograms ##############
#--------------- Monolith --------------------
mono_ads_per_year <-
  ggplot(monolith_time, aes(x = year, fill = year)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ads_per_month <-
  ggplot(monolith_time, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ad_counts2020 <-
  ggplot(mono2020, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ad_counts2021 <-
  ggplot(mono2021, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ad_counts2022 <-
  ggplot(mono2022, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ads_per_month_year <-
  ggplot(monolith_time, aes(x = Month, fill = year)) +
  geom_histogram(stat = "count") +
  labs(title = "Number of Ads Released per Month from 2020 - 2022 According to FbMonolith",
       x = "Month",
       y = "Number of Ads")

mono_ads_per_day_histo <-
  ggplot(monolith_time, aes(x = year_month_day, fill = year)) +
  geom_histogram(stat = "count") +
  labs(title = "Number of Ads Released per Day from 2020 - 2022 According to FbMonolith",
       x = "Date",
       y = "Number of Ads")

mono_ads_per_day_density <-
  ggplot(monolith_time, aes(x = year_month_day, fill = year)) +
  # geom_histogram(stat = "count") +
  labs(title = "Density of Ads Released per Day from Day from 2020 - 2022 According to FbMonolith",
       x = "Date",
       y = "Relative Ad Release Density") +
  geom_density(alpha = .8)

#--------------- Propublica --------------------
pb_ads_per_year <- ggplot(pb_time, aes(x = year, fill = year)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

pb_ads_per_month <- ggplot(pb_time, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

pb_ad_counts2017 <- ggplot(pb2017, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

pb_ad_counts2018 <- ggplot(pb2018, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

pb_ad_counts2019 <- ggplot(pb2019, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

pb_ads_per_month <- ggplot(pb_time, aes(x = month)) +
  geom_histogram(stat = "count")

pb_ads_per_month_year <-
  ggplot(pb_time, aes(x = Month, fill = year)) +
  geom_histogram(stat = "count") + 
  labs(title = "Number of Ads Released per Month from 2017 - 2019 According to ProPublica",
       x = "Month",
       y = "Number of Ads")

pb_ads_per_day_histo <-
  ggplot(pb_time, aes(x = year_month_day, fill = year)) +
  geom_histogram(stat = "count") +
  labs(title = "Number of Ads Released per Day from 2017 - 2019 According to ProPublica",
       y = "Number of Ads",
       x = "Date")

pb_ads_per_day_density <-
  ggplot(pb_time, aes(x = year_month_day, fill = year)) +
  # geom_histogram(stat = "count") +
  labs(title = "Number of Ads Released per Day from 2017 - 2019 According to ProPublica",
       x = "Date",
       y = "Relative Ad Release Density") +
  geom_density(alpha = .8)
