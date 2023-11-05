################################################################################
################################################################################
### Define MOUD (naltrexone)
### Author: Kat Hoffman July 2023
################################################################################
################################################################################


# Set up ------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(data.table)
library(tictoc)
library(here)

# proj_dir <- "projects/define_moud"

dts_cohorts <- open_dataset("../data/clean/create_cohort/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:32, length.out=n()))

td <- "/home/data/12201/" # directory of interest

# Read in RXL (pharmacy line)
files <- paste0(list.files(td, pattern = "TAFRXL", recursive = TRUE))
rxl <- open_dataset(paste0(td, files), format="parquet")

# read in OTL (other services line)
files <- paste0(list.files(td, pattern = "TAFOTL", recursive = TRUE))
otl <- open_dataset(paste0(td, files), format="parquet")


# Filter RXL files ---------------------------------------------------------

# extract Nal injection scripts
nal_scripts_rxl <- 
    rxl |>
    filter(NDC == "65757030001") |>
    select(BENE_ID,
           RX_FILL_DT) |>
    distinct() |>
    collect() |>
    mutate(moud_med = "nal",
           form = "injection",
           moud_start_dt = RX_FILL_DT,
           moud_end_dt = moud_start_dt + 21 + 30) |> # nal injections last 30 days
    select(BENE_ID, moud_med, form, moud_start_dt, moud_end_dt)


# write_rds(nal_scripts_rxl, "data/oud_info/nal/nal_scripts_rxl.rds")

# nal injections
nal_scripts_otl <-
    otl |>
    filter(NDC == "65757030001") |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID,
           LINE_SRVC_BGN_DT) |>
    distinct() |>
    collect() |>
    mutate(moud_med = "nal",
           form = "injection",
           moud_start_dt = LINE_SRVC_BGN_DT,
           moud_end_dt = moud_start_dt + 21 + 30) |> # nal injections last 30 days
    select(BENE_ID, moud_med, form, moud_start_dt, moud_end_dt)


# saveRDS(nal_scripts_otl, "data/oud_info/nal/nal_scripts_otl.rds")


# pull HCPCS codes --------------------------------------------------------

nal_hcpcs <- c(
    "J2315",
    "G2073"
    # "H0033" NOT USING,  too vague
)

nal_hcpcs_otl <-
    otl |>
    filter(LINE_PRCDR_CD %in% nal_hcpcs) |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT),
    ) |>
    select(BENE_ID,
           STATE_CD, 
           LINE_SRVC_BGN_DT) |>
    collect() |>
    mutate(moud_med = "nal",
           form = "injection",
           moud_start_dt = LINE_SRVC_BGN_DT,
           moud_end_dt = moud_start_dt + 21 + 30) |> # nal injections last 30 days
    select(BENE_ID, moud_med, form, moud_start_dt, moud_end_dt)
    
# Combine all sources of nal moud
all_nal <- full_join(nal_scripts_otl, nal_scripts_rxl) |>
    full_join(nal_hcpcs_otl) |>
    distinct(BENE_ID, moud_start_dt, moud_end_dt) |>
    group_by(BENE_ID) |>
    arrange(BENE_ID, moud_start_dt)

# adjudicate the naltrexone information depending on when the beneficiary received the next dose
# check when the next/last injection was given, and how long that was since the last injection ended (injection + 28 days)

# a start date for Nal should be:
#   - injection date if it's their first time receiving Nal, or
#   - injection date if they've received Nal previously and more than 28 + 21 days has passed since the previous injection
# an end date for Nal should be:
#   - injection date + 28 + 21 days if it's their last time ever receiving Nal, or
#   - injection date + 28 +21 days if there's more than 28+21 days between the current injection date and the next injection date

all_nal_adj <-
    all_nal |>
    drop_na(BENE_ID) |>
    mutate(lag_moud_end_dt = lag(moud_end_dt), # when did the last nal end?
           days_since_last_moud = as.integer(difftime(moud_start_dt, lag_moud_end_dt, units="days")),
           lead_moud_start_dt = lead(moud_start_dt), # when is the next dose starting compared to last moud end date
           days_to_next_moud = as.integer(difftime(lead_moud_start_dt, moud_end_dt, units="days")))  |>
    ungroup()|>
    # indicator of whether to use the moud_start_dt/moud_end_dt or ignore them
    mutate(use_start_dt = case_when(is.na(days_since_last_moud) ~ 1,
                                    days_since_last_moud > 0 ~ 1,
                                    TRUE ~ 0),
           
           use_end_dt = case_when(is.na(days_to_next_moud) ~ 1,
                                  days_to_next_moud > 0 ~ 1,
                                  TRUE ~ 0))

# keep only the needed rows for start dates
all_nal_start_dts <-
    all_nal_adj |>
    filter(use_start_dt == 1) |>
    select(BENE_ID, moud_start_dt) |>
    ungroup()

# keep only the needed rows for end dates (same N and order as all_nal_start_dts)
all_nal_end_dts <-
    all_nal_adj |>
    filter(use_end_dt == 1) |>
    select(BENE_ID, moud_end_dt) |>
    ungroup()

# merge to one final nal start/stop data set
all_nal_start_stop <-
    all_nal_start_dts |>
    bind_cols(all_nal_end_dts |> select(-BENE_ID)) |>
    mutate(moud_med = "nal") |>
    select(BENE_ID, moud_med, everything())|>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt)) |>
    filter(!(moud_end_dt < washout_start_dt)) |> # filter out rows that end before washout period begins
    select(-washout_start_dt)

# create interval indicators
nal_intervals <-
    all_nal_start_stop |>
    left_join(dts_cohorts) |> # left_join, only merge with rows that have any nal
    # start or end date can fall in the periods of interst
    mutate(moud_start_dt = case_when(moud_start_dt < washout_start_dt ~ washout_start_dt, TRUE ~ moud_start_dt),
           oud_moud_nal_washout_cal = case_when(moud_start_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                moud_end_dt %within% interval(washout_start_dt, washout_cal_end_dt) ~ 1,
                                                TRUE ~ 0),
           oud_moud_nal_washout_12mos_cal = case_when(moud_start_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                      moud_end_dt %within% interval(washout_start_dt, washout_12mos_end_dt) ~ 1,
                                                      TRUE ~ 0),
           oud_moud_nal_washout_cont = case_when(moud_start_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                 moud_end_dt %within% interval(washout_start_dt, washout_cont_end_dt) ~ 1,
                                                 TRUE ~ 0),
           oud_moud_nal_study_cal = case_when(moud_start_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                              moud_end_dt %within% interval(washout_start_dt, study_cal_end_dt) ~ 1,
                                              TRUE ~ 0),
           oud_moud_nal_study_cont = case_when(moud_start_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                               moud_end_dt %within% interval(washout_start_dt, study_cont_end_dt) ~ 1,
                                               TRUE ~ 0),
    ) |>
    group_by(BENE_ID) |>
    summarize(across(starts_with("oud_moud"), max)) # keep a row for each BENE_ID that denotes whether they had the drug in this period

all_nal_intervals <- 
    dts_cohorts |>
    select(BENE_ID) |>
    left_join(nal_intervals) |>
    mutate(across(contains("oud_moud"), ~ifelse(is.na(.x), 0, .x))) # replace missing data with zero (no nal in that interval)

write_rds(all_nal_intervals, "projects/create_cohort/data/oud_info/nal/all_nal_intervals.rds")


