################################################################################
################################################################################
###  CREATE OPIOID PAIN PRESCRIPTION VARIABLES
###  Kat Hoffman, March 2023
###  Purpose: clean TAFOTH and TAFIPH files for opioid_pain NDC codes
###  Output: cleaned data file containing minimum date the beneficiary ("data/final/opioid_pain_rx.rds")
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

opioid_pain_codes <- read_csv("input/NDC_codes/opioid_pain_rxs_clean.csv")

# Get minimum fill dates from RXL -----------------------------------------

tic()
opioid_pain_rxl <- 
    rxl |>
    select(BENE_ID, RX_FILL_DT, NDC, NDC_QTY, DAYS_SUPPLY)|>
    filter(NDC %in% opioid_pain_codes$ndc) |>
    rename(ndc = NDC) |>
    collect() |>
    left_join(opioid_pain_codes)
toc()

# Note this is filtering out all other modes of BUP (Film, injection -- we assume these are MOUD)
opioid_pain_rxl_no_bup <-
    opioid_pain_rxl  |>
    filter(drug_name != "Buprenorphine") |>
    select(BENE_ID, RX_FILL_DT) |>
    distinct()

opioid_pain_rxl_bup <-
    opioid_pain_rxl |>
    filter(drug_name == "Buprenorphine", dosage_form == "TABLET") |>
    mutate(pills_per_day = NDC_QTY/DAYS_SUPPLY,
           strength_per_day = bup_strength_clean * pills_per_day) |>
    filter(strength_per_day <= 10) |>
    select(BENE_ID, RX_FILL_DT)  |>
    distinct()

opioid_pain_rxl_clean <-
    full_join(opioid_pain_rxl_bup,
          opioid_pain_rxl_no_bup) |>
    inner_join(dts_cohorts)  # inner join to get the washout dates
    
earliest_opioid_pain_rxl <-
    opioid_pain_rxl_clean |>
    filter(RX_FILL_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |> 
    arrange(RX_FILL_DT) |>
    filter(row_number() == 1) |>
    select(BENE_ID, min_opioid_pain_dt = RX_FILL_DT) # only keep minimum washout date

# Get minimum fill dates from OTL -----------------------------------------

tic()
opioid_pain_otl <- 
    otl |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID, LINE_SRVC_BGN_DT, LINE_SRVC_END_DT, NDC, NDC_QTY)|>
    filter(NDC %in% opioid_pain_codes$ndc) |>
    rename(ndc = NDC) |>
    collect() |>
    left_join(opioid_pain_codes) # inner join to get the washout dates
toc()


# Note this is filtering out all other modes of BUP (Film, injection -- we assume these are MOUD)
opioid_pain_otl_no_bup <-
    opioid_pain_otl  |>
    filter(drug_name != "Buprenorphine") |>
    select(BENE_ID, LINE_SRVC_BGN_DT) |>
    distinct()

opioid_pain_otl_bup <-
    opioid_pain_otl |>
    filter(drug_name == "Buprenorphine", dosage_form == "TABLET") |>
    filter(bup_strength_clean <= 10) |> ## Note there are no days supply for the other services, so just have to assume <= 10mg doses are for pain
    select(BENE_ID, LINE_SRVC_BGN_DT)  |>
    distinct()

opioid_pain_otl_clean <-
    full_join(opioid_pain_otl_bup,
              opioid_pain_otl_no_bup) |>
    inner_join(dts_cohorts)  # inner join to get the washout dates


earliest_opioid_pain_otl <-
    opioid_pain_otl_clean |>
    filter(LINE_SRVC_BGN_DT >= washout_start_dt) |> # only keep codes after washout date
    group_by(BENE_ID) |>
    arrange(LINE_SRVC_BGN_DT) |>
    filter(row_number() == 1) |>
    select(BENE_ID, min_opioid_pain_dt = LINE_SRVC_BGN_DT) # only keep minimum washout date

# Determine earliest fill dates from OTL and RXL -----------------------------------------

tic()
earliest_opioid_pains <- 
    bind_rows(earliest_opioid_pain_otl, earliest_opioid_pain_rxl) |>
    arrange(min_opioid_pain_dt) |>
    distinct(BENE_ID, .keep_all = T) # keep only the first opioid_pain date
toc()

# Create indicator variables and save file -----------------------------------------

all_opioid_pains <-
    dts_cohorts |>
    left_join(earliest_opioid_pains) |>
    rename(opioid_pain_dt = min_opioid_pain_dt) |>
    mutate(opioid_pain_washout_cal = case_when(opioid_pain_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                 TRUE ~ 0),
           opioid_pain_washout_12mos_cal = case_when(opioid_pain_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                               TRUE ~ 0),
           opioid_pain_washout_cont = case_when(opioid_pain_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                  TRUE ~ 0),
           opioid_pain_study_cal = case_when(opioid_pain_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                               TRUE ~ 0),
           opioid_pain_study_cont = case_when(opioid_pain_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                                TRUE ~ 0)) |>
    select(BENE_ID,opioid_pain_dt, opioid_pain_washout_cal, opioid_pain_washout_12mos_cal, opioid_pain_washout_cont, opioid_pain_study_cal,  opioid_pain_study_cont)

saveRDS(all_opioid_pains, "data/final/opioid_pains.rds")
