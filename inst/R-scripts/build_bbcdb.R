# Author Peter W Setter
# Build a database of BBC News Summary articles
# Input "../../data/BBC New Summary/News Articles"

library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(DBI)
library(MonetDBLite)

dbdir <- "../../data/bbcdb"

con <- dbConnect(MonetDBLite::MonetDBLite(), dbdir)

file_name <- '../../data/BBC News Summary/News Articles/politics/001.txt'

article <- readLines(file_name) %>% 
  .[. != '']

topic <- str_extract(file_name, '(?<=News Articles/).*(?=/)')
article_num <- str_extract(file_name, '\\d{3}')
article_id <- paste0(topic, article_num)

data_frame(
  line = seq(1, length(article)),
  text = article
) %>% 
  View()



