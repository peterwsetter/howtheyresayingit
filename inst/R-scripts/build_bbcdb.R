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

#' Declare tables
#' articles includes article_id, topic, headline, and lead
#' article_content includes paragraphs, will be used for further tokenization

dbSendQuery(con,
              "CREATE TABLE articles (
              article_id varchar(20) PRIMARY KEY,
              topic varchar(10),
              headline text,
              lead text
              )")

dbSendQuery(con,
            "CREATE TABLE article_content (
            article_id varchar(20),
            paragraph_num int,
            paragraph_text text)
            ")

# Define function to write information to database

process_file <- function(file_name, db_con) {
  article <- readLines(file_name) %>% 
    .[. != '']
  
  topic <- str_extract(file_name, '(?<=News Articles/).*(?=/)')
  article_num <- str_extract(file_name, '\\d{3}')
  article_id <- paste0(topic, article_num)
  
  data_frame(article_id = article_id,
             topic = topic,
             headline = article[1],
             lead = article[2]) %>% 
    dbWriteTable(db_con, 'articles', value = ., append = TRUE)
  
  data_frame(paragraph_text = article[3:length(article)]) %>% 
    tibble::rowid_to_column('paragraph_num') %>% 
    mutate(article_id = article_id) %>% 
    select(article_id, paragraph_num, paragraph_text) %>% 
    dbWriteTable(db_con, 'article_content', value = ., append = TRUE)
}

