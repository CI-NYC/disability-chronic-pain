### extracting unique ndcs for nick to use with RX Norm

# Set up -----------------------------------------------------------------------
library(arrow)
library(tidyverse)
library(lubridate)
library(data.table)
library(tictoc)

td <- "/home/data/12201/" # directory of interest

# Read in RXL (pharmacy line)
files <- paste0(list.files(td, pattern = "TAFRXL", recursive = TRUE))
rxl <- open_dataset(paste0(td, files), format="parquet")

#  Read in OTL (Other services line) 
files <- paste0(list.files(td, pattern = "TAFOTL", recursive = TRUE))
otl <- open_dataset(paste0(td, files), format="parquet")

# washout/study dates of cohort
dts_cohorts <- 
    open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect()

tic()
unique_ndcs_rxl <- rxl |> 
    distinct(NDC) |>
    collect()
toc()

tic()
unique_ndcs_otl <- otl |> 
    distinct(NDC) |>
    collect()
toc()

all_ndcs <- bind_rows(unique_ndcs_rxl, unique_ndcs_otl) |>
    distinct()

saveRDS(all_ndcs, "data/tmp/all_unique_ndcs.rds")
