################################################################################
################################################################################
###  ADD LONG TERM CARE VARIABLES
###  Kat Hoffman, Jan 2023
###  Purpose: pull LTC variables from TAFOTH and TAFDEMC files for all years
###  Output: cohort file (int8) with LTC beneficiaries removed
################################################################################
################################################################################

# Set up -----------------------------------------------------------------------

# load libraries
library(arrow)
library(tidyverse)
library(lubridate)
library(data.table)

# read in tafdebse data base (all years)
td <- "/home/data/12201/" # directory of interest
dmc_files <- paste0(list.files(td, pattern = "*TAFDEMC_*", recursive = TRUE)) # files of interest
dmc <- open_dataset(paste0(td, dmc_files), format="parquet", partition = "year") # arrow dataset


dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:17, length.out=n()))

# extract relevant LTC variable from dems
ltc_dmc <- dmc |>
  select(BENE_ID, LTC_PIHP_MOS, RFRNC_YR) |>
  collect()

ltc_dmc_y1 <-
  ltc_dmc |>
  group_by(BENE_ID) |>
  arrange(RFRNC_YR) |>
  filter(row_number() == 1) |>
  ungroup() |>
  filter(LTC_PIHP_MOS > 0) |> # exclude any patients with any LTC (limitation: this could be after washout period)
  select(BENE_ID) |>
  mutate(cohort_exclusion_ltc_dmc = 1)

# read in tafihp data base (all years)
td <- "/home/data/12201/" # directory of interest
iph_files <- paste0(list.files(td, pattern = "*TAFIPH*", recursive = TRUE)) # files of interest
iph <- open_dataset(paste0(td, iph_files), format="parquet") # arrow dataset


ltc_cd <-
  c(
    "03", # = Discharged/transferred to skilled nursing facility (SNF) with Medicare certification in anticipation of covered skilled care â€” (For hospitals with an approved swing bed arrangement, use Code 61 â€” swing bed. For reporting discharges/transfers to a non-certified SNF, the hospital must use Code 04 â€” ICF.
    "04", # = Discharged/transferred to intermediate care facility (ICF).
    "40", # = Expired at home (hospice claims only)
    "41", # = Expired in a medical facility such as hospital, SNF, ICF, or freestanding hospice. (Hospice claims only)
    "42", # = Expired â€” place unknown (Hospice claims only)
    "50", # = Discharged/transferred to a Hospice â€” home.
    "51", # = Discharged/transferred to a Hospice â€” medical facility.
    "63", # = Discharged/transferred to a long-term care hospital. (eff. 1/2002)
    "64", # = Discharged/transferred to a nursing facility certified under Medicaid but not under Medicare (eff. 10/2002)
    "83", # = Discharged/transferred to a skilled nursing facility (SNF) with Medicare certification with a planned acute care hospital inpatient readmission.
    "84", # = Discharged/transferred to a facility that provides custodial or supportive care with a planned acute care hospital inpatient readmission.
    "85", # = Discharged/transferred to a designated cancer center or childrenâ€™s hospital with a planned acute care hospital inpatient readmission.
    "88", # = Discharged/transferred to a federal health care facility with a planned acute care hospital inpatient readmission.
    "92", # = Discharged/transferred to a nursing facility certified under Medicaid but not certified under Medicare with a planned acute care hospital inpatient readmission.
    "95"
  ) # = Discharged/transferred to another type of health care institution not defined elsewhere in this code list with a planned acute care hospital inpatient readmission."

# extract service *end* date, and discharge code from inpatient hospital
dc_cd <-
  iph |>
  mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
  select(BENE_ID, SRVC_BGN_DT, PTNT_DSCHRG_STUS_CD) |> 
  filter(PTNT_DSCHRG_STUS_CD %in% ltc_cd) |>
  collect()

# filter out patients with a LTC discharge code (defined by Floriana) within the 6 month washout period
# https://docs.google.com/spreadsheets/d/1j_rubKPLuEqi_TIZNm2l-VtRXpAoKyY0F_yUgwIO5EQ/edit#gid=0
ltc_exclusion_iph_cal <- 
  dc_cd |>
  left_join(dts_cohorts) |>
  filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cal_end_dt)) |>
  distinct(BENE_ID) |> # keep only unique beneficiary IDs
  mutate(cohort_exclusion_cal_ltc_iph = 1) |>
  full_join(ltc_dmc_y1) |> # merge with the variable from the LTC months
  mutate(cohort_exclusion_cal_ltc = case_when(cohort_exclusion_cal_ltc_iph == 1 ~ 1,
                                              cohort_exclusion_ltc_dmc == 1 ~ 1,
                                              TRUE ~ 0
                                              )) |>
    select(BENE_ID, cohort_exclusion_cal_ltc)

ltc_exclusion_iph_12mos_cal <- 
    dc_cd |>
    left_join(dts_cohorts) |>
    filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_12mos_end_dt)) |>
    distinct(BENE_ID) |> # keep only unique beneficiary IDs
    mutate(cohort_exclusion_cal_ltc_iph = 1) |>
    full_join(ltc_dmc_y1) |> # merge with the variable from the LTC months
    mutate(cohort_exclusion_12mos_cal_ltc = case_when(cohort_exclusion_cal_ltc_iph == 1 ~ 1,
                                                cohort_exclusion_ltc_dmc == 1 ~ 1,
                                                TRUE ~ 0
    )) |>
    select(BENE_ID, cohort_exclusion_12mos_cal_ltc)

ltc_exclusion_iph_cont <- 
    dc_cd |>
    left_join(dts_cohorts) |>
    filter(SRVC_BGN_DT %within% interval(washout_start_dt, washout_cont_end_dt)) |>
    distinct(BENE_ID) |> # keep only unique beneficiary IDs
    mutate(cohort_exclusion_cont_ltc_iph = 1)|>
    full_join(ltc_dmc_y1) |> # merge with the variable from the LTC months
    mutate(cohort_exclusion_cont_ltc = case_when(cohort_exclusion_cont_ltc_iph == 1 ~ 1,
                                                cohort_exclusion_ltc_dmc == 1 ~ 1,
                                                TRUE ~ 0)) |>
    select(BENE_ID, cohort_exclusion_cont_ltc)
    
cohort_exclusion_ltc <- full_join(ltc_exclusion_iph_cont, ltc_exclusion_iph_cal) |>
    full_join(ltc_exclusion_iph_12mos_cal)
write_rds(cohort_exclusion_ltc, "data/tafoth/cohort_exclusion_ltc.rds")

