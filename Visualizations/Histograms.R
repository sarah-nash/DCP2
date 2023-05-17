library(dplyr)
library(ggplot2)
library(lubridate)
library(wordcloud2)
library(wordcloud)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

################## Counts Histograms ##############
#--------------- Monolith --------------------
mono_ads_per_year <- ggplot(monolith_time, aes(x = year, fill = year)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ads_per_month <- ggplot(monolith_time, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ad_counts2020 <- ggplot(mono2020, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ad_counts2021 <- ggplot(mono2021, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ad_counts2022 <- ggplot(mono2022, aes(x = month, fill = month)) +
  geom_histogram(stat = "count") +
  theme(legend.position = "none")

mono_ads_per_month_year <- ggplot(monolith_time, aes(x = month, fill = year)) +
  geom_histogram(stat = "count")

mono_ads_per_month <- ggplot(monolith_time, aes(x = month)) +
  geom_histogram(stat = "count")

mono_ads_per_day <- ggplot(monolith_time, aes(x = year_month_day, fill = year)) +
  geom_histogram(stat = "count")

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

pb_ads_per_month_year <- ggplot(pb_time, aes(x = month, fill = year)) +
  geom_histogram(stat = "count")

pb_ads_per_day <- ggplot(pb_time, aes(x = year_month_day, fill = year)) +
  geom_histogram(stat = "count")
################# Barplots ###############
#-------------- Monolith ----------------
mono_age_barplot <- ggplot(mono_age_groups, aes(x = rownames(mono_age_groups), y = t.mono_age_groups.)) +
  geom_bar(stat = "identity")

#-------------- Propublica ---------------
ggplot(pb_time, aes(x = target_minage)) +
  geom_bar(stat = "count")

ggplot(pb_time, aes(x = target_maxage)) +
  geom_bar(stat = "count")

pb_age_barplot <- ggplot(pb_age_groups, aes(x = rownames(pb_age_groups), y = t.pb_age_groups.)) +
  geom_bar(stat = "identity")
