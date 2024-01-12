#############################################################################
## Dinamica_subset_control_table: Subset the control table to remove simulations
## that have already been completed
## Date: 01-10-2022, 16-11-2023
## Author: Ben Black, Carlson Büth
#############################################################################

#set working directory
setwd(wpath)

#load control table
Control_table <- read.csv(Control_table_path)

#subset to non-completed simulations
Control_table <- Control_table[Control_table$Completed.string == "N",]

# Load $LULCC_START_ROW and $LULCC_END_ROW
# check if both are set
if (Sys.getenv("LULCC_START_ROW") == "" || Sys.getenv("LULCC_END_ROW") == "") {
  stop(paste0("LULCC_START_ROW and LULCC_END_ROW must be set. ",
              "Got LULCC_START_ROW = '", Sys.getenv("LULCC_START_ROW"),
              "' and ",
              "LULCC_END_ROW = '", Sys.getenv("LULCC_END_ROW"), "'"))
}
# [START_ROW, END_ROW[
LULCC_START_ROW <- Sys.getenv("LULCC_START_ROW") # string
LULCC_END_ROW <- Sys.getenv("LULCC_END_ROW") # string
# check that both represent positive integers
if (!grepl("^[0-9]+$", LULCC_START_ROW) || !grepl("^[0-9]+$", LULCC_END_ROW)) {
  stop(paste0("LULCC_START_ROW and LULCC_END_ROW must be positive integers. ",
              "Got LULCC_START_ROW = '", Sys.getenv("LULCC_START_ROW"),
              "' and ",
              "LULCC_END_ROW = '", Sys.getenv("LULCC_END_ROW"), "'"))
}
# convert to numeric
LULCC_START_ROW <- as.numeric(LULCC_START_ROW)
LULCC_END_ROW <- as.numeric(LULCC_END_ROW)

# subset table
Control_table <- Control_table[LULCC_START_ROW:(LULCC_END_ROW - 1),]
