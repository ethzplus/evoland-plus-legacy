library(CodeDepends)

Script_paths <- list.files("E:/LULCC_CH_HPC/Scripts", full.names = TRUE, recursive = TRUE)

test_read <- getInputs(CodeDepends::readScript(Script_paths[1]))

#vector names of required packages
req_packs <- c("data.table",
"raster",
"tidyverse",
"plyr",
"readr",
"sjmisc",
"terra",
"rgdal",
"rgeos",
"future.apply",
"future",
"future.callr",
"stringr",
"stringi",
"readxl",
"rlist",
"rstatix",
"openxlsx",
"sp",
"xlsx",
"callr",
"gdata",
"randomForest",
"caret")

#check what version of each pacakge is installed

