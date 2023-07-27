################################################################################
################################################################################
###  CREATE A DATA TABLE FOR MISSINGNESS NUMBERS
###  Kat Hoffman, JUNE 2023
###  Output: latex table with outcomes of missingness for final cohort analyzed
################################################################################
################################################################################

# Create descriptive tables
library(tidylog)
library(tidyverse)
library(janitor)
library(arrow)
library(gtsummary)
library(gt)
library(tictoc)

analysis_cohort <-  read_rds("data/final/analysis_cohort.rds")
dat_lmtp <- read_rds("data/final/dat_lmtp.rds")

actual_missing_tbl <-
    dat_lmtp |>
    select(contains("_na")) |>
    tbl_summary()

latex_tbl <-
    actual_missing_tbl |>
    as_gt() |>
    as_latex()

# export to latex code
write_rds(latex_tbl, "output/missingness_tbl2_gt.rds")


tbl_tmp <-
    analysis_cohort |>
    select(# disability_pain_cal,
           # demographics
           dem_age,
           dem_sex,
           dem_race_cond,
           dem_married_or_partnered,
           dem_primary_language_english,
           dem_household_size,         
           dem_veteran,
           # dem_tanf_benefits,         
           # dem_ssi_benefits,
           # comorbidities/prescriptions in washout
           bipolar_washout_cal,
           anxiety_washout_cal,
           adhd_washout_cal,
           depression_washout_cal,
           mental_ill_washout_cal) |>
    mutate(across(everything(), ~ case_when(is.na(.x) ~ 1, TRUE ~ 0))) |>
    tbl_summary()

latex_tbl <-
    tbl_tmp |>
    as_gt() |>
    as_latex()

# export to latex code
write_rds(latex_tbl, "output/missingness_tbl_gt.rds")


