################################################################################
################################################################################
###  ADD OUD PROBABLE MISUSE VARIABLE
###  Kat Hoffman, Feb 2023
###  Purpose: define OUD via various definitions
###  Output: 
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(data.table)
library(tictoc)
library(future)
library(furrr)
library(doParallel)
# options(cores=50)
registerDoParallel()
plan(multicore)
getDoParWorkers()

# Load necessary files ---------------------------------------------------------

dts_cohorts <- open_dataset("projects/create_cohort/data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:32, length.out=n()))

opioids <- read_csv("projects/create_cohort/input/NDC_codes/opioid_pain_rxs_clean.csv") |>
    rename(NDC = ndc)

#ndc_list_clean <- read_rds( "input/NDC_codes/ndc_list_clean.rds")
#bup_ndc_dose_clean <- read_rds("input/NDC_codes/bup_ndc_dose_clean.rds")

td <- "/home/data/12201/" # directory of interest

# Read in RXL (pharmacy line)
files <- paste0(list.files(td, pattern = "TAFRXL", recursive = TRUE))
rxl <- open_dataset(paste0(td, files), format="parquet")

files <- paste0(list.files(td, pattern = "TAFRXH", recursive = TRUE))
rxh <- open_dataset(paste0(td, files), format="parquet")

# extract prescriber / dispenser info from cohort of interest
tic()
prescribers <- 
    rxh |>
    select(BENE_ID, CLM_ID,
           PRSCRBNG_PRVDR_ID, PRSCRBNG_PRVDR_NPI,
           DSPNSNG_PRVDR_ID, DSPNSNG_PRVDR_NPI,
           RX_FILL_DT) |>
   collect()
toc()

tic()
# extract drug info from cohort of interest and categorize script types
ndcs <- rxl |>
    select(BENE_ID, CLM_ID, contains("NDC"), DAYS_SUPPLY)|>
    filter(NDC %in% opioids$NDC) |>
    filter(!is.na(BENE_ID)) |>
    collect() |>
    left_join(opioids)
toc() # 133 seconds

prescribers_filter <- 
    prescribers |>
    filter(CLM_ID %in% ndcs$CLM_ID)

saveRDS(prescribers_filter, "data/tmp/prescribers_filter.rds")

all_opioids <- inner_join(ndcs, prescribers_filter) # only want to keep prescribers  

saveRDS(all_opioids, "data/oud_info/all_opioids_prescribers_ndcs.rds")


all_opioids <- read_rds("data/oud_info/all_opioids_prescribers_ndcs.rds")

opioids_rxl_no_bup  <-
    all_opioids |>
    filter(!(drug_name == "Buprenorphine"))

# for bup, only keep strength/day < 10
opioid_pain_rxl_bup <-
    all_opioids |>
    filter(drug_name == "Buprenorphine", dosage_form == "TABLET") |>
    mutate(pills_per_day = NDC_QTY/DAYS_SUPPLY,
           strength_per_day = bup_strength_clean * pills_per_day) |>
    filter(strength_per_day <= 10) 

all_opioids_clean <-
    opioids_rxl_no_bup |>
    select(BENE_ID, RX_FILL_DT, PRSCRBNG_PRVDR_NPI, DSPNSNG_PRVDR_NPI, DAYS_SUPPLY) |>
    full_join( opioid_pain_rxl_bup |> select(BENE_ID, RX_FILL_DT, PRSCRBNG_PRVDR_NPI, DSPNSNG_PRVDR_NPI, DAYS_SUPPLY)) |>
    left_join(dts_cohorts |> select(BENE_ID, washout_start_dt))

#Probable Opioid Misuse:  
# number of opioid prescribers (<= 2 prescribers=0, 3-4 prescribers=1, >= 5 prescribers=2), 
# number of pharmacies used for medication filling (<= 2 pharmacies=0, 3-4 pharmacies=1, >= 5 pharmacies=2),
# days supplied of short-acting opioids, 
# and days of supply of long-acting opioids (<=185 days=0, 186-240 days=1, >240 days=2)
# over 6-month periods, summed and >= 5 categorized as probable misuse

rolling_windows <- function(df, month_start){
    print(paste0(month_start, Sys.time()) )
    df |>
        # first, define the 6 month time window of interest
        mutate(start_month = washout_start_dt + months(month_start),
               end_month = washout_start_dt + months(month_start) + 182) |>
        # then, filter the prescriptions to only contain those filled within the relevant time frame
        filter(RX_FILL_DT %within% interval(start_month, end_month)) |>
        group_by(BENE_ID) |>
        # count the number of unique 1) prescribing provider NPIs 2) dispensing providing NPIs and
        # 3) sum the total days supply of all opioids given in the window
        summarize(distinct_providers = n_distinct(PRSCRBNG_PRVDR_NPI),
                  distinct_dispensers = n_distinct(DSPNSNG_PRVDR_NPI),
                  total_days_supply = sum(DAYS_SUPPLY)) |>
        ungroup() |>
        # classify the scores using the probable misuse variable logic 
        mutate(score_providers = case_when(distinct_providers <= 2 ~ 0,
                                           distinct_providers <= 4 ~ 1,
                                           distinct_providers >= 5 ~ 2),
               score_dispensers = case_when(distinct_dispensers <= 2 ~ 0,
                                            distinct_dispensers <= 4 ~ 1,
                                            distinct_dispensers >= 5 ~ 2),
               score_days_supply = case_when(total_days_supply <= 185 ~ 0,
                                             total_days_supply <= 240 ~ 1,
                                             total_days_supply > 240 ~ 2,
                                             is.na(total_days_supply) ~ 0 # a few days supply are missing
               ),
               # sum the scores - will be an integer between 0-6
               score_misuse = score_providers +  score_dispensers + score_days_supply) |>
        # create (start) month variable as the ID
        mutate(month = month_start) 
}

# # takes 9 min for month 0 to 6
# start_time <- Sys.time()
# tmp <- rolling_windows(opioids_check, 0)
# end_time <- Sys.time()
# time_elapsed <- difftime(end_time, start_time)
# time_elapsed # 9 min

# realized i also need to do month 0... and continue through max of 4 years on study (48-6 months = 42) because we don't know when cont time ends
still <- c(0:42) # may need to go back and add the last 6 months
foreach (time = still) %dopar% {
    tmp <- rolling_windows(all_opioids_clean, time)
    write_parquet(tmp, paste0("data/oud_info/tmp_splits_oud_misuse/", time, ".parquet"))
}

# reduce list output to one long data frame (only contains beneficiaries receiving any opioids)
all_misuse_splits <- open_dataset("projects/create_cohort/data/oud_info/tmp_splits_oud_misuse/")

all_misuse_splits_c <-
    all_misuse_splits |>
    collect()

# make a key that is number of months on study (only need to do this for continuous cohort)
study_months_key <-
    dts_cohorts |>
        mutate(max_study_months = interval(washout_start_dt, study_cont_end_dt) %/% months(1)) |>
    select(BENE_ID, max_study_months) |>
    # max months is 48 if they don't have a final study end date for the continuous enrollment interval
    replace_na(list(max_study_months = 42))  # don't need to fill in for no washout continuous completion because they'll be excluded

study_months_key

all_misuse_splits_c_adj <-
    all_misuse_splits_c |>
    left_join(study_months_key) |>
    filter(month <= max_study_months) # remove months that are greater than the max study (cont) months since washout

# determine eligibility criteria here
exclusion_misuse <-
    all_misuse_splits_c_adj  |>
    filter(month == 0) |>
    select(BENE_ID:score_misuse) |>
    mutate(cohort_exclusion_oud_misuse = case_when(score_misuse >= 5 ~ 1)) |>
    select(BENE_ID, cohort_exclusion_oud_misuse) |>
    right_join(dts_cohorts |> select(BENE_ID)) |>
    replace_na(list(cohort_exclusion_oud_misuse = 0))

study_cal_misuse <-
    all_misuse_splits_c_adj  |>
    filter(month < 18) |> # first 18 month rolling periods are indexed by 0:17... may need to fix this if we want to include info from after calendar years (months 25-31)
    select(BENE_ID:score_misuse) |>
    group_by(BENE_ID) |> # group by id to calculate the max score variables, and whether they had an oud misuse qualifying score in the calendar washout period
    mutate(max_score_misuse = max(score_misuse),
           oud_misuse_cal_any = case_when(max_score_misuse >= 5 ~ 1, TRUE ~ 0)) |>
    group_by(BENE_ID, oud_misuse_cal_any) |> # group by both variables so that we keep them during "summarize" step
    summarize(across(distinct_providers:total_days_supply, mean)) |>
    rename(oud_misuse_cal_avg_distinct_providers = distinct_providers,
           oud_misuse_cal_avg_distinct_dispensers = distinct_dispensers,
           oud_misuse_cal_avg_total_days_supply = total_days_supply)

study_cont_misuse <-
    all_misuse_splits_c_adj  |>
    select(BENE_ID:score_misuse) |>
    group_by(BENE_ID) |>
    group_by(BENE_ID) |> # group by id to calculate the max score variables, and whether they had an oud misuse qualifying score in the calendar washout period
    mutate(max_score_misuse = max(score_misuse),
           oud_misuse_cont_any = case_when(max_score_misuse >= 5 ~ 1, TRUE ~ 0)) |>
    group_by(BENE_ID, oud_misuse_cont_any) |> # group by both variables so that we keep them during "summarize" step
    summarize(across(distinct_providers:total_days_supply, mean))|>
    rename(oud_misuse_cont_avg_distinct_providers = distinct_providers,
           oud_misuse_cont_avg_distinct_dispensers = distinct_dispensers,
           oud_misuse_cont_avg_total_days_supply = total_days_supply)

oud_misuse_clean <- 
    exclusion_misuse |>
    left_join(study_cal_misuse) |>
    left_join(study_cont_misuse) |>
    replace_na(list( oud_misuse_cont_any = 0,
                     oud_misuse_cal_any = 0)) # Don't replace oud_misuse_* variables with NA because we don't want them to be displayed in the table

# save entire OUD misuse data frame in final folder
saveRDS(oud_misuse_clean, "projects/create_cohort/data/final/oud_misuse.rds")


