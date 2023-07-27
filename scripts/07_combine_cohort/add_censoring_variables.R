################################################################################
################################################################################
###  MERGE CENSORING VARIABLES
###  Kat Hoffman, March 2023
###  Output: one data set with all censoring variable indicators/dates
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(tidylog)
library(lubridate)
library(data.table)
library(janitor)
library(tictoc)
library(future)
library(furrr)
library(doParallel)
# options(cores=50)
registerDoParallel()
plan(multicore)
getDoParWorkers()

# read in tafdebse data base (all years)
td <- "/home/data/12201/" # directory of interest
dbs_files <- paste0(list.files(td, pattern = "*TAFDEBSE*", recursive = TRUE)) # files of interest
dbs <- open_dataset(paste0(td, dbs_files), format="parquet", partition = "year") # arrow dataset

td <- "/home/data/12201/" 
dts_files <- paste0(list.files(td, pattern = "*TAFDEDTS*", recursive = TRUE)) 
dts <- open_dataset(paste0(td, dts_files), format="parquet") # arrow dataset

dts_dates <-
    dts |>
    select(BENE_ID, ENRLMT_START_DT, ENRLMT_END_DT) |>
    arrange(BENE_ID, ENRLMT_START_DT) |>
    collect()

dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() 

# CENSORING BASED ON ENROLLMENT DATES

# filter so enrollment start or end date is within the calendar study duration
# first filter to last enrollment start date -- if enrollment end date is after calendar end date, not censored

tic()
censoring_df <- 
    dts_cohorts |>
    select(BENE_ID, washout_start_dt, study_cal_end_dt) |>
    group_by(BENE_ID) |>
    left_join(dts_dates) |>
    filter(ENRLMT_START_DT %within% interval(washout_start_dt, study_cal_end_dt)) |>
    filter(row_number() == n()) |>
    mutate(study_complete_dts_cal = case_when(ENRLMT_END_DT >= study_cal_end_dt ~ 1, TRUE ~ 0))
toc() # 6 hours

saveRDS(censoring_df, "data/tmp/censoring_df_tmp.rds")
censoring_df_tmp <- read_rds("data/tmp/censoring_df_tmp.rds")

censoring_df <- 
    censoring_df_tmp  |>
    mutate(censoring_dts_cal_dt = case_when(study_complete_dts_cal == 0 ~ ENRLMT_END_DT)) |>
    select(BENE_ID, study_complete_dts_cal, censoring_dts_cal_dt)

save(censoring_df, "data/final/censoring_dts.rds")
    
# CENSORING FOR DUAL ELIGIBILITY

tic()
dual_codes_to_check <-
    dbs |>
    select(BENE_ID, RFRNC_YR, starts_with("DUAL_ELGBL_CD")) |>
    collect()

# obtain the date for all dual eligibility encounters for all beneficiaries
all_duals <-
    dual_codes_to_check |>
    rename(year = RFRNC_YR) |>
    select(-DUAL_ELGBL_CD_LTST) |>
    pivot_longer(cols = starts_with("DUAL_ELGBL_CD"),
                 names_to = "month",
                 values_to = "dual_code",
                 values_drop_na = T)  |>
    filter(dual_code != "00") |>
    mutate(month = parse_number(month),
           year = as.numeric(year),
           dual_elig_date = as.Date(paste0(year, "-", month, "-01"))) |>
    select(BENE_ID,  dual_elig_date, dual_code)
toc()  #599.57 sec elapsed

# keep only first dual eligibility date -- also need to do this for the reg. eligibility codes indicating dual
first_dual <-
    all_duals |>
    group_by(BENE_ID) |>
    arrange(dual_elig_date) |>
    filter(row_number() == 1)

saveRDS(first_dual, "data/tmp/first_dual_tmp.rds")

first_dual_tmp <- readRDS("data/tmp/first_dual_tmp.rds")

elig_codes_to_check <-
    dbs |>
    select(BENE_ID, RFRNC_YR, starts_with("ELGBLTY_GRP_CD")) |>
    filter(!(is.na(BENE_ID))) |>
    collect()

dual_elig_cds <- c("23","24","25","26") 

dual_elig <-
    elig_codes_to_check |>
    rename(year = RFRNC_YR) |>
    select(-ELGBLTY_GRP_CD_LTST) |>
    pivot_longer(cols = starts_with("ELGBLTY_GRP_CD"),
                 names_to = "month",
                 values_to = "elig_code",
                 values_drop_na = T)  |>
    filter(elig_code %in% dual_elig_cds) |>
    mutate(month = parse_number(month),
           year = as.numeric(year),
           dual_elig_date = as.Date(paste0(year, "-", month, "-01"))) |>
    select(BENE_ID,  dual_elig_date, elig_code) |>
    group_by(BENE_ID) |>
    arrange(dual_elig_date) |>
    filter(row_number() == 1)

save(dual_elig, "data/tmp/dual_elig_cds.rds")

# take first code only

# eligibility code
dual_elig <- read_rds("data/tmp/dual_elig_cds.rds")
# dual codes
first_dual_tmp <- read_rds("data/tmp/first_dual_tmp.rds")

censoring_dual <-
    full_join(dual_elig, first_dual_tmp) |>
    arrange(dual_elig_date) |>
    group_by(BENE_ID) |>
    filter(row_number() == 1) |>
    select(BENE_ID, censoring_dual_dt = dual_elig_date)

saveRDS(censoring_dual, "data/final/censoring_dual.rds")

 
# CENSORING AGE

birth_dates_cols <-
    dbs |>
    arrange(RFRNC_YR) |>
    select(BENE_ID, BIRTH_DT, AGE) |>
    collect()

# keep only the first non NA birth dates
birth_dates <-
    birth_dates_cols |>
    drop_na(BIRTH_DT) |> # 46,894 missing a birth date before doing this
    distinct(BENE_ID, .keep_all = T) 

ages <-
    dts_cohorts |> 
    left_join(birth_dates) |>
    # note: this is slightly different from the "AGE" variable calculated by Medicaid (end of year age)
    mutate(age_enrollment = as.numeric(difftime(washout_start_dt, BIRTH_DT, units = "days") / 365.25))

censoring_ages <- 
    ages |>
    # get age at the end of the study
    mutate(age_study_end = difftime(study_cal_end_dt, BIRTH_DT, units = "days") / 365.25) |>
    filter(age_study_end > 65) |> # only work on beneficiaries who need to be censored for age (computational time)
    mutate(years_over_65 = as.numeric(age_study_end - 65)) |>
    mutate(days_over_65 = round(years_over_65*365.25), # get days over 65
           censoring_age_dt = study_cal_end_dt - days(days_over_65)) |> # remove those days from the study end date
    select(BENE_ID, censoring_age_dt) |>
    full_join(dts_cohorts |> select(BENE_ID))

saveRDS(censoring_ages, "data/final/censoring_age.rds")

#### COMBINE CENSORING VARS

censoring_ages <- read_rds("data/final/censoring_age.rds")
censoring_dual <- read_rds("data/final/censoring_dual.rds")
censoring_dts <- read_rds("data/final/censoring_dts.rds")

censoring_full <- full_join(censoring_ages, censoring_dts) |>
    left_join(censoring_dual)

censoring_full_add  <-
    censoring_full |>
    mutate(censoring_ever_dt = pmin(censoring_age_dt, censoring_dual_dt, censoring_dts_cal_dt, na.rm=T)) 

saveRDS(censoring_full_add, "data/final/censoring_full.rds")





