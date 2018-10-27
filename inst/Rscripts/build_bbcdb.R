# Author Peter W Setter
# Build a database of BBC News Summary articles
# Input ../../

library(DBI)

dbdir <- "../../data/bbcdb"

con <- dbConnect(MonetDBLite::MonetDBLite(), dbdir)

