################################################################################
################################################################################
###  EXPORT COHORT EXCLUSION DATA FOR FLOWCHARTS
###  Kat Hoffman, APRIL 2023
###  Output: cohort exclusion count tables
################################################################################
################################################################################

# Create descriptive tables

library(tidyverse)
library(tictoc)

cohort_exclusions <- read_rds("projects/create_cohort/data/final/cohort_exclusions_df.rds")

tic()
desc_cohort_exclusions <-
    cohort_exclusions |>
        select(
    cohort_exclusion_age,
    cohort_exclusion_sex,
    cohort_exclusion_state,
    cohort_exclusion_deaf,
    cohort_exclusion_blind,
    cohort_exclusion_cont_deafblind,
    cohort_exclusion_cont_no_washout, # no end of washout period
    cohort_exclusion_cont_no_elig, # no eligibility criteria
    cohort_exclusion_cont_unknown_disability, # unsure whether they have a disability from eligibility code
    cohort_exclusion_cont_dual, # dual eligible
    cohort_exclusion_cont_pregnancy,
    cohort_exclusion_cont_ltc,
    cohort_exclusion_cont_itlcdsbl,
    cohort_exclusion_cont_dementia, 
    cohort_exclusion_cont_schiz, 
    cohort_exclusion_cont_speech, 
    cohort_exclusion_cont_cerpals, 
    cohort_exclusion_cont_epilepsy, 
    cohort_exclusion_cont_pall, 
    cohort_exclusion_cont_cancer,
    cohort_exclusion_cont_institution, # hospice, institution, LTC?
    cohort_exclusion_cont_tb) |>  # tuberculosis
# note we aren't including OUD exclusion criteria for the continuous cohort

    group_by_all() |>
    count()
toc()

saveRDS(desc_cohort_exclusions, "projects/create_cohort/output/desc_cohort_exclusions.rds")


tic()
analysis_cohort_exclusions <-
    cohort_exclusions |>
    select(cohort_exclusion_cal_jan2016,
               cohort_exclusion_age,
               cohort_exclusion_sex,
               cohort_exclusion_state,
               cohort_exclusion_deaf,
               cohort_exclusion_blind,
               cohort_exclusion_cal_deafblind,
               cohort_exclusion_cal_no_washout, # no end of washout period
               cohort_exclusion_cal_no_elig, # no eligibility criteria
               cohort_exclusion_cal_unknown_disability, # unsure whether they have a disability from eligibility code
               cohort_exclusion_cal_dual, # dual eligible
               cohort_exclusion_cal_pregnancy,
               cohort_exclusion_cal_ltc,
               cohort_exclusion_cal_itlcdsbl,
               cohort_exclusion_cal_dementia, 
               cohort_exclusion_cal_schiz, 
               cohort_exclusion_cal_speech, 
               cohort_exclusion_cal_cerpals, 
               cohort_exclusion_cal_epilepsy, 
               cohort_exclusion_cal_pall, 
               cohort_exclusion_cal_cancer,
               cohort_exclusion_cal_institution, # hospice, institution, LTC?
               cohort_exclusion_cal_tb,  # tuberculosis
               # Add OUD variables for this cohort
               cohort_exclusion_oud_cal) |>
    group_by_all() |>
    count()
toc()



saveRDS(analysis_cohort_exclusions, "projects/create_cohort/output/analysis_cohort_exclusions.rds")
