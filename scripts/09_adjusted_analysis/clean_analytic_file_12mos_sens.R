################################################################################
################################################################################
###  CLEAN ANALYSES FOR 12 MONTHS SENSITIVITY ANALYSES
###  Author Kat Hoffman, May 2023
###  Purpose: show examples of analysis using tmle_mlr3.R script
###  Output: fit object with psi, standard error, g, Q, influence curve estimates
################################################################################
################################################################################

# Set up -----------------------------------------------------------

library(tidyverse)
library(fastDummies)
library(janitor)
library(tictoc)
library(gtsummary)
library(gt)

set.seed(7) # for random sampling

# Read in analytical file ------------------------------------------------------

analysis_cohort <- read_rds("data/final/sens_12mos_analysis_cohort.rds")

# define outcomes ---------------------------------------------------------

dat_lmtp_outcomes_tmp <-
    analysis_cohort |> 
    # create completeoring variables:
    # complete is standard censoring indicator (incomplete follow-up = 0, complete = 1)
    # complete2 allows people with event but incomplete follow=up to have event
    mutate(
           complete = case_when(is.na(censoring_ever_dt) ~ 1, TRUE ~ 0),
           ### OUD CAL
           complete2_oud = case_when(complete == 1 ~ 1, # if study is complete, 1
                                             oud_cal == 1 ~ 1, # or, if they have the event, 1
                                             TRUE ~ 0), # otherwise incomplete
           # event indicators
           oud_event = case_when(complete == 1 ~ oud_cal, TRUE ~ NA_real_),
           oud_event2 = case_when(complete2_oud == 1 ~ oud_cal, TRUE ~ NA_real_),

           ### OUD POISON CAL
           complete2_oud_poison = case_when(complete == 1 ~ 1, # if study is complete, 1
                                            oud_poison_study_cal == 1 ~ 1, # or, if they have the event, 1
                                         TRUE ~ 0), # otherwise incomplete
           # event indicators
           oud_poison_event = case_when(complete == 1 ~ oud_poison_study_cal, TRUE ~ NA_real_),
           oud_poison_event2 = case_when(complete2_oud_poison == 1 ~ oud_poison_study_cal, TRUE ~ NA_real_),
           ### OUD ABUSE CAL
           complete2_oud_abuse = case_when(complete == 1 ~ 1, # if study is complete, 1
                                            oud_abuse_study_cal == 1 ~ 1, # or, if they have the event, 1
                                            TRUE ~ 0), # otherwise incomplete
           # event indicators
           oud_abuse_event = case_when(complete == 1 ~ oud_abuse_study_cal, TRUE ~ NA_real_),
           oud_abuse_event2 = case_when(complete2_oud_abuse == 1 ~ oud_abuse_study_cal, TRUE ~ NA_real_),
           
           ### OUD ABUSE (HILLARY = primary outcome ) CAL
           complete2_oud_hillary = case_when(complete == 1 ~ 1, # if study is complete, 1
                                           oud_hillary_study_cal == 1 ~ 1, # or, if they have the event, 1
                                           TRUE ~ 0), # otherwise incomplete
           # event indicators
           oud_hillary_event = case_when(complete == 1 ~ oud_hillary_study_cal, TRUE ~ NA_real_),
           oud_hillary_event2 = case_when(complete2_oud_hillary == 1 ~ oud_hillary_study_cal, TRUE ~ NA_real_)
          
           )


dat_lmtp_outcomes <-
    dat_lmtp_outcomes_tmp |>
    select(BENE_ID,
           oud_cal,
           oud_abuse_study_cal,
           oud_hillary_study_cal,
           oud_poison_study_cal,
           any_moud_cal,
           contains("complete"),
           contains("event"))
            

dat_lmtp_raw <-
    analysis_cohort |>
        select(
        BENE_ID,
        # exposure
        disability_pain_12mos_cal,
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
        bipolar_washout_12mos_cal,
        anxiety_washout_12mos_cal,
        adhd_washout_12mos_cal,
        depression_washout_12mos_cal,
        mental_ill_washout_12mos_cal,
        # antidepressant_washout_cal,
        # benzodiazepine_washout_cal,
        # antipsychotic_washout_cal,
        # stimulant_washout_cal,
        # moodstabilizer_washout_cal
        )


bin_predictors <-
    dat_lmtp_raw |>
    select(dem_sex:mental_ill_washout_12mos_cal) |> # run on all predictors except age (cont.)
    names()

dat_lmtp_predictors <-
    dat_lmtp_raw |>
    dummy_cols(bin_predictors, # creates indicators for missingness / categorical variables for all dichotomous predictors
        remove_first_dummy = T, # remove first column (colliniarity)
        remove_selected_columns = T) |> # remove original columns (don't need)
     mutate(across(-BENE_ID, # replace missing values with 0 (indicators already created)
                  ~ifelse(is.na(.x), 0, .x)),
            disability_pain_12mos_cal = factor(disability_pain_12mos_cal)
            ) |>
    clean_names() |> # clean up column names
    rename(BENE_ID = bene_id)   # except bene_id

# filter out ages due to positivity violations
dat_lmtp <-
    dat_lmtp_predictors |>
    full_join(dat_lmtp_outcomes) |>
    filter(dem_age >= 35) # only keep people >= 35 for positivity violation reasons

# dat_lmtp <- bind_rows(random_samp, others) # decided not to do this

dat_lmtp |>
    count(disability_pain_12mos_cal)

saveRDS(dat_lmtp, "data/final/dat_lmtp_sens_12mos.rds")

# saveRDS(dat_lmtp, "data/final/dat_lmtp_12mos.rds")

# make a descriptive table ------------------------------------------------

lmtp_tbl <- 
    dat_lmtp |>
    select(starts_with("oud_"),
           any_moud_cal,
           starts_with("dem_"),
           everything(),
           -BENE_ID) |>
    tbl_summary(by = disability_pain_cal) |>
    add_overall()

lmtp_tbl_df <-
    lmtp_tbl |>
    as_gt() |>
    as.data.frame() |>
    select(label:stat_4)

write.csv(lmtp_tbl_df, "tbls/analysis_tbl.csv")
    
