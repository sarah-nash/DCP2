library(dplyr)
library(ggplot2)
library(lubridate)
library(tidyverse)
library(tidytext)
library(tm)
library(forcats)
library(glue)

##########################  Recommended Objects in this File #############################
#         ****   You can call any of these by highlighting one, then Shift + Enter   ****

# mono_20pages  and  pb_20pages - Plots of Top 20 Sponsors (labeled as pages and titles in the data)
# mono2020_20pages, mono2021_20pages, mono2022_20pages  - Yearly top20 Sponsors from Monolith

# pb2017_20pages, pb2018_20pages, pb2019_20pages - Yearly top20 Sponsors from Monolith
# mono_sponsor_distributions("h")  and  pb_sponsor_distributions("h") 
#   ^^^^^ These ^^^^^ functions will print histogram distributions for all of the top20 sponsors for each year

# mono_sponsor_distributions("l")  and  pb_sponsor_distributions("l")
#   ^^^^^ These ^^^^^ functions will print line plot distributions for all of the top20 sponsors for each year

##########################################################################################




##################### Pages and Sponsors ###########
#------------------ Monolith ----------------
mono_20pages <-
  ggplot(mono_top20_pages, aes(
    x = fct_reorder(page_name, n),
    y = n,
    fill = page_name
  )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors according to FbMonolith (2020 - 2022)") +
  coord_flip()

# -------- Yearly ---------

mono2020_20pages <-
  ggplot(mono2020_top20_pages,
         aes(
           x = fct_reorder(page_name, n),
           y = n,
           fill = page_name
         )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors of 2020 According to FbMonolith") +
  coord_flip()

mono2021_20pages <-
  ggplot(mono2021_top20_pages,
         aes(
           x = fct_reorder(page_name, n),
           y = n,
           fill = page_name
         )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors of 2021 According to FbMonolith") +
  coord_flip()

mono2022_20pages <-
  ggplot(mono2022_top20_pages,
         aes(
           x = fct_reorder(page_name, n),
           y = n,
           fill = page_name
         )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors of 2022 According to FbMonolith") +
  coord_flip()

#------------------ ProPublica ----------------
pb_20pages <-
  ggplot(pb_top20_pages, aes(
    x = fct_reorder(title, n),
    y = n,
    fill = title
  )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors According to Propublica (2017 - 2019)") +
  coord_flip()

# -------- Yearly ---------

pb2017_20pages <-
  ggplot(pb2017_top20_pages, aes(
    x = fct_reorder(title, n),
    y = n,
    fill = title
  )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors of 2017 According to Propublica") +
  coord_flip()

pb2018_20pages <-
  ggplot(pb2018_top20_pages, aes(
    x = fct_reorder(title, n),
    y = n,
    fill = title
  )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors of 2018 According to Propublica") +
  coord_flip()

pb2019_20pages <-
  ggplot(pb2019_top20_pages, aes(
    x = fct_reorder(title, n),
    y = n,
    fill = title
  )) +
  geom_col() +
  theme(legend.position = "none",
        axis.text.y = element_text(face = "bold")) +
  labs(x = "",
       y = "Count",
       title = "Top 20 Sponsors of 2019 According to Propublica") +
  coord_flip()



#------------ See Daily Distributions Each top20 sponsor ----------------
#------------------ Monolith ----------------
mono_sponsor_distributions <- function (type) {
  if(type == "h") {
    for (i in mono_top20_pages$page_name) {
      arr <- mono_top_page_day %>%
        filter(page_name == i)
      
      plt <-
        ggplot(arr, aes(x = year_month_day, y = max_n, fill = page_name)) +
        geom_col() +
        theme(legend.position = "none") +
        labs(
          x = "Date",
          y = "Count",
          title = glue("Ads by {i} Each Day According to FbMonolith")
        )
      print(plt)
    }
  }
  else if(type == "l"){
    for (i in mono_top20_pages$page_name) {
      arr <- mono_top_page_day %>%
        filter(page_name == i)
      
      plt <-
        ggplot(arr, aes(x = year_month_day, y = max_n, group = 1)) +
        geom_line() +
        theme(legend.position = "none") +
        labs(
          x = "Date",
          y = "Count",
          title = glue("Ads by {i} Each Day According to FbMonolith")) +
        theme_gray()
      print(plt)
    }
  }
}

#------------------ ProPublica ----------------
pb_sponsor_distributions <- function(type) {
  if (type == "h") {
    for (i in pb_top20_pages$title) {
      arr <- pb_top_page_day %>%
        filter(title == i)
      
      plt <-
        ggplot(arr, aes(x = year_month_day, y = max_n, fill = title)) +
        geom_col() +
        theme(legend.position = "none") +
        labs(
          x = "Date",
          y = "Count",
          title = glue("Ads by {i} Each Day According to Propublica")
        )
      print(plt)
    }
  }
  else if (type == "l") {
    for (i in pb_top20_pages$title) {
      arr <- pb_top_page_day %>%
        filter(title == i)
      
      plt <-
        ggplot(arr, aes(x = year_month_day, y = max_n)) +
        geom_line() +
        theme(legend.position = "none") +
        labs(
          x = "Date",
          y = "Count",
          title = glue("Ads by {i} Each Day According to Propublica")
        )
      print(plt)
    }
  }
}
