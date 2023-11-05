################################################################################
################################################################################
###  CREATE Benzodiazepine PRESCRIPTION VARIABLES
###  Kat Hoffman, Sept 2023
###  Purpose: clean TAFOTH and TAFIPH files for benzodiazepine NDC codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/benzodiazepine_rx.rds")
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
    open_dataset("projects/create_cohort/data/tafdedts/dts_cohorts.parquet") |>
    collect()

codes <- read_rds("projects/create_cohort/input/NDC_codes/mediation_unsafe_pain_mgmt_benzo_ndc.rds") # these use the RXNORM method NDC values


# Get minimum fill dates from RXL -----------------------------------------

tic()
codes_rxl <- 
    rxl |>
    select(BENE_ID, RX_FILL_DT, NDC)|>
    filter(NDC %in% codes$NDC) |>
    collect() |>
    inner_join(dts_cohorts) # inner join to get the washout dates
toc()

tic()
earliest_codes_rxl <-
    codes_rxl |>
    filter(RX_FILL_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |> 
    arrange(RX_FILL_DT) |>
    filter(row_number() == 1)
toc()

earliest_codes_rxl_clean <-
    earliest_codes_rxl |>
    select(BENE_ID, min_code_dt = RX_FILL_DT) # only keep minimum washout date

# Get minimum fill dates from OTL -----------------------------------------

tic()
codes_otl <- 
    otl |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID, LINE_SRVC_BGN_DT, NDC)|>
    filter(NDC %in% codes$NDC) |>
    collect() |>
    inner_join(dts_cohorts) # inner join to get the washout dates
toc()

tic()
earliest_codes_otl <-
    codes_otl |>
    filter(LINE_SRVC_BGN_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |>
    arrange(LINE_SRVC_BGN_DT) |>
    filter(row_number() == 1)
toc()

earliest_codes_otl_clean <-
    earliest_codes_otl |>
    select(BENE_ID, min_code_dt = LINE_SRVC_BGN_DT) # only keep minimum washout date

# Determine earliest fill dates from OTL and RXL -----------------------------------------

tic()
earliest_codes <- 
    bind_rows(earliest_codes_otl_clean, earliest_codes_rxl_clean) |>
    arrange(min_code_dt) |>
    distinct(BENE_ID, .keep_all = T) # keep only the first code date
toc()

# Create indicator variables and save file -----------------------------------------

all_codes <-
    dts_cohorts |>
    left_join(earliest_codes) |>
    rename(benzodiazepine_rxnorm_dt = min_code_dt) |>
    mutate(benzodiazepine_rxnorm_washout_cal = case_when(benzodiazepine_rxnorm_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                 TRUE ~ 0),
           benzodiazepine_rxnorm_washout_12mos_cal = case_when(benzodiazepine_rxnorm_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                  TRUE ~ 0),
           benzodiazepine_rxnorm_washout_cont = case_when(benzodiazepine_rxnorm_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                  TRUE ~ 0),
           benzodiazepine_rxnorm_study_cal = case_when(benzodiazepine_rxnorm_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                               TRUE ~ 0),
           benzodiazepine_rxnorm_study_cont = case_when(benzodiazepine_rxnorm_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                                TRUE ~ 0)) |>
    select(BENE_ID,benzodiazepine_rxnorm_dt, benzodiazepine_rxnorm_washout_cal, benzodiazepine_rxnorm_washout_12mos_cal, 
           benzodiazepine_rxnorm_washout_cont, benzodiazepine_rxnorm_study_cal,  benzodiazepine_rxnorm_study_cont)

saveRDS(all_codes, "projects/create_cohort/data/final/benzodiazepines_rxnorm.rds")
