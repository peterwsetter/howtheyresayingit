# Tokenize & Identify Quotes

library(dplyr)
library(tidyr)
library(tidytext)
library(DBI)

dbdir <- "../../data/bbcdb"

con <- dbConnect(MonetDBLite::MonetDBLite(), dbdir)

# Pull text into working memory
quotes_separated <- tbl(con, 'quotes_separated') %>% 
  collect() 

data(stop_words)

# Tokenize paragraphs into individual words
quotes_separated %>% 
  gather(key = quoted, value = text, quote, nonquote) %>% 
  filter(text != '') %>% 
  unnest_tokens(output = wrd,
                input = text,
                token = 'words',
                to_lower = FALSE) %>% 
  group_by(article_id, paragraph_num, sentence_num) %>% 
  mutate(word_num = row_number()) %>% 
  ungroup() ->
  article_tokenized


dbWriteTable(con, 'article_tokenized', article_tokenized)

dbDisconnect(con, shutdown=TRUE)
