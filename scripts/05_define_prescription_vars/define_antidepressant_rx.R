################################################################################
################################################################################
###  CREATE ANTIDEPRESSANT PRESCRIPTION VARIABLES
###  Kat Hoffman, March 2023
###  Purpose: clean TAFOTH and TAFIPH files for antidepressant NDC codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/antidepressant_rx.rds")
###        has a adhd ICD code in the study duration
###         and indicators of whether it occurs in washout or overall study duration
################################################################################
################################################################################

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

antidepressant_codes <- read_csv("input/NDC_codes/antidepressants_clean.csv")


# Get minimum fill dates from RXL -----------------------------------------

tic()
antidepressant_rxl <- 
    rxl |>
    select(BENE_ID, RX_FILL_DT, NDC)|>
    filter(NDC %in% antidepressant_codes$ndc) |>
    collect() |>
    inner_join(dts_cohorts) # inner join to get the washout dates
toc()

tic()
earliest_antidepressant_rxl <-
    antidepressant_rxl |>
    filter(RX_FILL_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |> 
    arrange(RX_FILL_DT) |>
    filter(row_number() == 1)
toc()

earliest_antidepressant_rxl_clean <-
    earliest_antidepressant_rxl |>
    select(BENE_ID, min_antidepressant_dt = RX_FILL_DT) # only keep minimum washout date

# Get minimum fill dates from OTL -----------------------------------------

tic()
antidepressant_otl <- 
    otl |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID, LINE_SRVC_BGN_DT, NDC)|>
    filter(NDC %in% antidepressant_codes$ndc) |>
    collect() |>
    inner_join(dts_cohorts) # inner join to get the washout dates
toc()

tic()
earliest_antidepressant_otl <-
    antidepressant_otl |>
    filter(LINE_SRVC_BGN_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |>
    arrange(LINE_SRVC_BGN_DT) |>
    filter(row_number() == 1)
toc()

earliest_antidepressant_otl_clean <-
    earliest_antidepressant_otl |>
    select(BENE_ID, min_antidepressant_dt = LINE_SRVC_BGN_DT) # only keep minimum washout date

# Determine earliest fill dates from OTL and RXL -----------------------------------------

tic()
earliest_antidepressants <- 
    bind_rows(earliest_antidepressant_otl_clean, earliest_antidepressant_rxl_clean) |>
    arrange(min_antidepressant_dt) |>
    distinct(BENE_ID, .keep_all = T) # keep only the first antidepressant date
toc()

# Create indicator variables and save file -----------------------------------------

all_antidepressants <-
    dts_cohorts |>
    left_join(earliest_antidepressants) |>
    rename(antidepressant_dt = min_antidepressant_dt) |>
    mutate(antidepressant_washout_cal = case_when(antidepressant_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                           TRUE ~ 0),
           antidepressant_washout_12mos_cal = case_when(antidepressant_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                        TRUE ~ 0),
           antidepressant_washout_cont = case_when(antidepressant_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                            TRUE ~ 0),
           antidepressant_study_cal = case_when(antidepressant_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                         TRUE ~ 0),
           antidepressant_study_cont = case_when(antidepressant_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                          TRUE ~ 0)) |>
           select(BENE_ID,antidepressant_dt, antidepressant_washout_cal, antidepressant_washout_12mos_cal, antidepressant_washout_cont, antidepressant_study_cal,  antidepressant_study_cont)
           
saveRDS(all_antidepressants, "data/final/antidepressants.rds")
