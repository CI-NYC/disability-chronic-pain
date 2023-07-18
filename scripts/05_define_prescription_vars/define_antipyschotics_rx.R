################################################################################
################################################################################
###  CREATE ANTIPSYCHOTIC PRESCRIPTION VARIABLES
###  Kat Hoffman, March 2023
###  Purpose: clean TAFOTH and TAFIPH files for antipsychotic NDC codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/antipsychotic_rx.rds")
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

antipsychotic_codes <- read_csv("input/NDC_codes/antipsychotics_clean.csv")


# Get minimum fill dates from RXL -----------------------------------------

tic()
antipsychotic_rxl <- 
    rxl |>
    select(BENE_ID, RX_FILL_DT, NDC)|>
    filter(NDC %in% antipsychotic_codes$ndc) |>
    collect() |>
    inner_join(dts_cohorts) # inner join to get the washout dates
toc()

tic()
earliest_antipsychotic_rxl <-
    antipsychotic_rxl |>
    filter(RX_FILL_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |> 
    arrange(RX_FILL_DT) |>
    filter(row_number() == 1)
toc()

earliest_antipsychotic_rxl_clean <-
    earliest_antipsychotic_rxl |>
    select(BENE_ID, min_antipsychotic_dt = RX_FILL_DT) # only keep minimum washout date

# Get minimum fill dates from OTL -----------------------------------------

tic()
antipsychotic_otl <- 
    otl |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID, LINE_SRVC_BGN_DT, NDC)|>
    filter(NDC %in% antipsychotic_codes$ndc) |>
    collect() |>
    inner_join(dts_cohorts) # inner join to get the washout dates
toc()

tic()
earliest_antipsychotic_otl <-
    antipsychotic_otl |>
    filter(LINE_SRVC_BGN_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |>
    arrange(LINE_SRVC_BGN_DT) |>
    filter(row_number() == 1)
toc()

earliest_antipsychotic_otl_clean <-
    earliest_antipsychotic_otl |>
    select(BENE_ID, min_antipsychotic_dt = LINE_SRVC_BGN_DT) # only keep minimum washout date

# Determine earliest fill dates from OTL and RXL -----------------------------------------

tic()
earliest_antipsychotics <- 
    bind_rows(earliest_antipsychotic_otl_clean, earliest_antipsychotic_rxl_clean) |>
    arrange(min_antipsychotic_dt) |>
    distinct(BENE_ID, .keep_all = T) # keep only the first antipsychotic date
toc()

# Create indicator variables and save file -----------------------------------------

all_antipsychotics <-
    dts_cohorts |>
    left_join(earliest_antipsychotics) |>
    rename(antipsychotic_dt = min_antipsychotic_dt) |>
    mutate(antipsychotic_washout_cal = case_when(antipsychotic_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                  TRUE ~ 0),
           antipsychotic_washout_12mos_cal = case_when(antipsychotic_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                        TRUE ~ 0),
           antipsychotic_washout_cont = case_when(antipsychotic_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                   TRUE ~ 0),
           antipsychotic_study_cal = case_when(antipsychotic_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                                TRUE ~ 0),
           antipsychotic_study_cont = case_when(antipsychotic_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                                 TRUE ~ 0)) |>
    select(BENE_ID,antipsychotic_dt, antipsychotic_washout_cal, antipsychotic_washout_12mos_cal, antipsychotic_washout_cont, antipsychotic_study_cal,  antipsychotic_study_cont)

saveRDS(all_antipsychotics, "data/final/antipsychotics.rds")
