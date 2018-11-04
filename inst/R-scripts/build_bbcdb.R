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
            paragraph_text text,
            PRIMARY KEY(article_id, paragraph_num))
            ")

# Define function to write information to database

process_file <- function(file_name, db_con) {
  # file_name character string of file_path
  # db_con DBI connection object
  
  # readLines reads in a text file, breaking into a vector on '\n'
  # The second line removes empty strings caused by the double '\n' 
  # for paragrah breaks
  article <- readLines(file_name) %>% 
    .[. != '']
  
  # Files are separated by topic which is in the folder name
  topic <- str_extract(file_name, '(?<=News Articles/).*(?=/)')
  # Article number is the file name
  article_num <- str_extract(file_name, '\\d{3}')
  # Combine for unique id
  article_id <- paste0(topic, article_num)
  
  # Write article information
  data_frame(article_id = article_id,
             topic = topic,
             headline = article[1],
             lead = article[2]) %>% 
    dbWriteTable(db_con, 'articles', value = ., append = TRUE)
  
  # The paragraphs are identified number
  # Write to the database
  data_frame(paragraph_text = article[3:length(article)]) %>% 
    tibble::rowid_to_column('paragraph_num') %>% 
    mutate(article_id = article_id) %>% 
    select(article_id, paragraph_num, paragraph_text) %>% 
    dbWriteTable(db_con, 'article_content', value = ., append = TRUE)
}


# Get list of files to process
list.files(path = '../../data/BBC News Summary/News Articles',
           full.names = TRUE,
           recursive = TRUE) %>% 
  # Write data to database
  map(~process_file(.x, con))

dbDisconnect(con, shutdown=TRUE)
