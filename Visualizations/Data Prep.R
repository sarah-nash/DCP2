library(dplyr)
library(ggplot2)
library(lubridate)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)

##############################  **Run This File First** ##################################

# This file contains all objects and data frame dependencies for other vizualization files

##############################  **Run This File First** ##################################



################# Initialization ####################
df_monolith <- read.csv("fb_monolith_with_targets.csv")
df_pb <- read.csv("pro_pub_targets_entire.csv")
df_monolith$observed_at <- as.Date(df_monolith$observed_at)
df_pb$created_at <- as.Date(df_pb$created_at)

################ Data with Timestamps ############
#--------------- Monolith -------------
monolith_time <- df_monolith %>%
  mutate(
    year_month_day = lubridate::floor_date(observed_at, "day"),
    year = format(observed_at, "%Y"),
    month = format(observed_at, "%m"),
    Month = format(observed_at, "%b"),
    day = format(observed_at, "%d")
  )

monolith_time$Month <- factor(monolith_time$Month, levels = month.abb)

monolith_time$page_name[monolith_time$page_name == "Doctors Without Borders/Médecins Sans Frontières (MSF)"] <-
  "Doctors Without Borders/Medecins Sans Frontieres (MSF)"
#--------------- Propublica -----------------
pb_time <- df_pb %>%
  mutate(
    year_month_day = lubridate::floor_date(created_at, "day"),
    year = format(created_at, "%Y"),
    month = format(created_at, "%m"),
    Month = format(created_at, "%b"),
    day = format(created_at, "%d")
  )

pb_time$Month <- factor(pb_time$Month, levels = month.abb)
############## Groupings #################
#------------ Monolith Years ---------------
mono2020 <- monolith_time %>%
  filter(year == 2020)

mono2021 <- monolith_time %>%
  filter(year == 2021)

mono2022 <- monolith_time %>%
  filter(year == 2022)

#----------- Propublica Years ------------
pb2017 <- pb_time %>%
  filter(year == 2017)

pb2018 <- pb_time %>%
  filter(year == 2018)

pb2019 <- pb_time %>%
  filter(year == 2019)

#----------- Monolith political value averages ------------
mono_average_daily_political_value <- monolith_time %>%
  group_by(year_month_day) %>%
  summarise(average_political = mean(political_value))

mono20_average_daily_political_value <- mono2020 %>%
  group_by(year_month_day) %>%
  summarise(average_political = mean(political_value))

mono21_average_daily_political_value <- mono2021 %>%
  group_by(year_month_day) %>%
  summarise(average_political = mean(political_value))

mono22_average_daily_political_value <- mono2022 %>%
  group_by(year_month_day) %>%
  summarise(average_political = mean(political_value))

#------------ Propublica political value --------
############ Targets ######################
#--------------- Monolith Age Groups -------------
target_age <- data.frame(str_split(monolith_time$target_age, "-"))

age <- data.frame(t(target_age))

mono_age <- monolith_time %>%
  mutate(min_age = age$X1,
         max_age = age$X2)


mono_age_groups <- mono_age %>%
  summarize(
    "<18" = sum(min_age < 18),
    "18-24" = sum(min_age < 24 & max_age > 18),
    "25-34" = sum(min_age < 34 & max_age > 25),
    "35-44" = sum(min_age < 44 & max_age > 35),
    "45-54" = sum(min_age < 54 & max_age > 45),
    "55-64" = sum(min_age < 64 & max_age > 55),
    "65+" = sum(max_age > 64)
  )


mono_age_groups <- data.frame(t(mono_age_groups))

#--------------- Propublica Age Groups -------------
# Age groups
pb_age_groups <- pb_time %>%
  summarize(
    "<18" = sum(target_minage < 18, na.rm = TRUE),
    "18-24" = sum(target_minage < 24 &
                    target_maxage > 18, na.rm = TRUE),
    "25-34" = sum(target_minage < 34 &
                    target_maxage > 25, na.rm = TRUE),
    "35-44" = sum(target_minage < 44 &
                    target_maxage > 35, na.rm = TRUE),
    "45-54" = sum(target_minage < 54 &
                    target_maxage > 45, na.rm = TRUE),
    "55-64" = sum(target_minage < 64 &
                    target_maxage > 55, na.rm = TRUE),
    "65+" = sum(target_maxage > 64, na.rm = TRUE)
  )

pb_age_groups <- data.frame(t(pb_age_groups))

#------------- Monolith Pages ----------------
mono_page_freq <- monolith_time %>%
  count(page_name) %>%
  arrange(desc(n))

mono_top20_pages <- head(mono_page_freq, 20)

# Time Groups
mono_top_page_day <- monolith_time %>%
  group_by(year_month_day) %>%
  count(page_name) %>%
  summarize(page_name, max_n = max(n))

mono_page_yearly_freq <- monolith_time %>%
  group_by(year) %>%
  count(page_name) %>%
  arrange(desc(n))

# 2020 sponsors
mono2020_pages <- mono_page_yearly_freq %>%
  filter(year == 2020)

mono2020_top20_pages <- head(mono2020_pages, 20)

#2021 Sponsors
mono2021_pages <- mono_page_yearly_freq %>%
  filter(year == 2021)

mono2021_top20_pages <- head(mono2021_pages, 20)

#2022 sponsors
mono2022_pages <- mono_page_yearly_freq %>%
  filter(year == 2022)

mono2022_top20_pages <- head(mono2022_pages, 20)

#------------- Propublica Pages ----------------
pb_page_freq <- pb_time %>%
  count(title) %>%
  arrange(desc(n))

pb_top20_pages <- head(pb_page_freq, 20)


# Time Groups
pb_top_page_day <- pb_time %>%
  group_by(year_month_day) %>%
  count(title) %>%
  summarize(title, max_n = max(n))

pb_page_yearly_freq <- pb_time %>%
  group_by(year) %>%
  count(title) %>%
  arrange(desc(n))

# 2017 sponsors
pb2017_pages <- pb_page_yearly_freq %>%
  filter(year == 2017)

pb2017_top20_pages <- head(pb2017_pages, 20)

#2018 Sponsors
pb2018_pages <- pb_page_yearly_freq %>%
  filter(year == 2018)

pb2018_top20_pages <- head(pb2018_pages, 20)

#2019 sponsors
pb2019_pages <- pb_page_yearly_freq %>%
  filter(year == 2019)

pb2019_top20_pages <- head(pb2019_pages, 20)
