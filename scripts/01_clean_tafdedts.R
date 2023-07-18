################################################################################
################################################################################
###  CLEAN DATES FILES
###  Kat Hoffman, Dec 2022
###  Purpose: clean TAFDEDTS files for 2016-2019
###  Output: data/tafdedts/dts_cohorts.parquet arrow file w/ ids, washout start date,
###         calendar enrollment washout end and study end dates,
###         continuous enrollment washout end and study end dates,
###         and relevant cohort exclusion criteria using the dates only
################################################################################
################################################################################

# Set up ------------------------------------------------------------------


# load libraries
library(arrow)
library(tidyverse)
library(lubridate)
library(tictoc)
library(future)
library(furrr)
library(doParallel)
# options(cores=50)
registerDoParallel()
plan(multicore)
getDoParWorkers()

td <- "/home/data/12201/" 
dts_files <- paste0(list.files(td, pattern = "*TAFDEDTS*", recursive = TRUE)) 
dts <- open_dataset(paste0(td, dts_files), format="parquet") # arrow dataset

tic()
dts_query <-  # 7 seconds
    dts |>
    filter(!is.na(BENE_ID)) |>
    select(BENE_ID, ENRLMT_START_DT, ENRLMT_END_DT) |>
    collect() |>
    group_by(BENE_ID) |>
    arrange(BENE_ID,  ENRLMT_START_DT)
toc()

###########################################################################
############ COHORT 1 #####################################################
###########################################################################

# Exclude everyone enrolled on jan 1 2016
# Study starts on date first enrolled (after jan 1 2016),
# Washout period is first 6 calendar months.
# Study time ends 18 calendar months after washout period or
# 24 calendar months after study time starts

tic() # takes 32 minutes
# study dates using calendar months (ignoring discontinuous enrollment)
dts_cal_cohort <-
    dts_query |>
    select(BENE_ID, ENRLMT_START_DT) |>
    distinct(BENE_ID, .keep_all = T) |>
    mutate(washout_start_dt = ENRLMT_START_DT,
           washout_end_dt = ENRLMT_START_DT + 182, # 6 months after first enrollment
           washout_12mos_end_dt = ENRLMT_START_DT + 365, # 6 months after first enrollment
           study_end_dt = ENRLMT_START_DT + 730) |> # 2 years after first enrollment
    select(-ENRLMT_START_DT)  |>
    ungroup()
toc()

saveRDS(dts_cal_cohort, "data/tafdedts/dts_cal_cohort.rds")

    

###########################################################################
############ COHORT 2 #####################################################
###########################################################################

# Big cohort
# Include everyone regardless of enrollment start date
# Study starts on date first enrolled
# Washout period is first 6 months of total enrollment
# (e.g. 2 months + 3 months + 1 month enrollment periods)
# Study ends after 24 months of total enrollment 
# (or 18 months total enrollment after washout period ends)
# This makes the assumption that SSDI/Medicaid are completely intertwined, 
# i.e. if the person stops one they stop the other.
# Now we have less time to look at people with a physical disability. 
# We can look at the distribution for folks with a disability of time 
# on medicaid and use that to decide on sampling/adjustment.

# nested by BENE_ID
tic() # 9 minutes
nest_dts <-
    dts_query |>
    nest() |>
    ungroup() |>
    mutate(worker_id = rep(seq(1:n_workers), length.out=n())) # create an ID for which worker to send that mini DF to
toc()

saveRDS(nest_dts, "data/tafdedts/nest_dts.rds")

nest_dts <- read_rds("data/tafdedts/nest_dts.rds")

# create a new index to save 1000 tmp files because worker_id was too slow (54)
nest_dts_index <- 
    nest_dts |>
    mutate(index = rep(seq(1:1000), length.out=n())) 

# save each file as a temporary split - will remove later
for (i in 1:1000){
    nest_dts_index |>
        filter(index == i) |>
        saveRDS(paste0("data/tafdedts/tmp_splits/", i, ".rds"))
}

fx <- function(data) {
    data |>
        arrange(ENRLMT_START_DT) |>
        # create an index for dates (to remove overlapping enrollment windows)
        mutate(indx = c(0, cumsum(as.numeric(lead(ENRLMT_START_DT)) >
                                      cummax(as.numeric(ENRLMT_END_DT)))[-n()])) %>%
        group_by(indx) %>%
        summarise(ENRLMT_START_DT = first(ENRLMT_START_DT), ENRLMT_END_DT = last(ENRLMT_END_DT)) |> # removes overlapping enrollment window
        select(-indx) |>  # get rid of index column
        mutate(days_enrollment = as.numeric(difftime( ENRLMT_END_DT, ENRLMT_START_DT, units="days"))) |> # compute cumulative days of enrollment (seconds_enrollment column is no longer here after using summarize
        mutate(total_enrollment = cumsum(days_enrollment)) |> # calculate cumulative days of enrollment
        mutate(washout_start_dt = min(ENRLMT_START_DT), # first start date for that beneficiary
               max_enrollment_dt = max(ENRLMT_END_DT), # last end date for the beneficiary
               extra_enrollmment_washout = total_enrollment - 182, # get the days after meeting washout time (6mo) to subtract
               extra_enrollmment_study = total_enrollment - 730, # get the days after meeting study time (2yr) to subtract
               a_washout_end_dt = case_when(total_enrollment >= 182 ~ ENRLMT_END_DT - days(extra_enrollmment_washout)), # subtract the extra days from the enrollment end date, if the beneficiary met the total enrollment days during this row
               a_study_end_dt = case_when(total_enrollment >= 730 ~ ENRLMT_END_DT - days(extra_enrollmment_study)),
               washout_end_dt = min(a_washout_end_dt, na.rm=T), # only keep the minimum date that they met the washout and study date
               study_end_dt = min(a_study_end_dt, na.rm=T), # keep only minimum study end date
               study_completed = case_when(is.infinite(study_end_dt) ~ 0, # 1 if study was completed
                                           TRUE ~ 1), 
               study_end_dt = case_when(is.infinite(washout_end_dt) ~ as.Date(NA_character_), # if didn't finish washout, no study end date -- also need to convert "infinite" NA's to true NA's in R otherwise this date returns wonky
                                        study_completed == 0 ~ max_enrollment_dt,
                                        TRUE ~ study_end_dt), # if they didn't complete 24 months enrollment, their study end date is their last enrollment date
               washout_end_dt = case_when(is.infinite(washout_end_dt) ~ as.Date(NA_character_), # correct "infinite" date to be a true NA value
                                          TRUE ~ washout_end_dt)
        ) |>
        select(washout_start_dt, washout_end_dt, study_end_dt, study_completed) |>  # keep only the first row -- they're all the same
        filter(row_number()==1)
}

# detect temporary splits (1:1000) not done yet
cleaned <- list.files("data/tafdedts/tmp_splits/", pattern = ".parquet")
num <- parse_number(cleaned)
still <- data.frame(all = 1:1000) |>
    mutate(isin = all %in% num) |>
    filter(isin==F) |>
    pull(all)

# run the function for all splits (1:1000 not cleaned yet), then save as a parquet file
tic()
foreach(i = seq(still)) %dopar%
    {idx <- still[i]
        print(idx)
        print(Sys.time())
        tic()
        tmp <- read_rds( paste0("data/tafdedts/tmp_splits/", idx, ".rds"))|>
            select(BENE_ID, data) |>
            mutate(stuff = future_map(data, fx)) |>
            select(BENE_ID, stuff) |>
            unnest()
        write_parquet(tmp, sink = paste0("data/tafdedts/tmp_splits/", idx, "_clean.parquet"))
        toc()
    }
toc()  

# gather all the split files
td <- "data/tafdedts/tmp_splits/" 
files <- paste0(list.files(td, pattern = "*parquet*", recursive = TRUE)) 
dts_splits <- open_dataset(paste0(td, files), format="parquet") # arrow dataset

# final cohort to save
tic()
dts_cont_cohort <- 
    dts_splits |>
    collect() |>
    mutate(cohort_exclusion_cont_no_washout = case_when(is.na(washout_end_dt) ~ 1, TRUE ~ 0)) |> # didn't finish washout period (in total enrollment time)
    rename(washout_cont_end_dt = washout_end_dt,
           study_cont_end_dt = study_end_dt) 
toc()

saveRDS(dts_cont_cohort, "data/tafdedts/dts_cont_cohort.rds") # save this large file of the continuous enrollment cohort


###########################################################################
############ MERGE TO ONE DATES FILE ######################################
###########################################################################

# read back in the cohorts 
dts_cont_cohort <- read_rds("data/tafdedts/dts_cont_cohort.rds")
dts_cal_cohort <- read_rds("data/tafdedts/dts_cal_cohort.rds")

dts_cal_cohort |> filter(study_end_dt >= as.Date("2020-01-01")) |> nrow()

# create exclusion criteria for calendar cohort, truncate calendar washout end and study end times if after end of 2019
# (note this was already fixed in the continuous enrollment cohort - used last enrollment date for study end date and marked study as incomplete)
dts_cohorts <-
    dts_cal_cohort |>
    # create calendar cohort exclusion indicators
    mutate(cohort_exclusion_cal_jan2016 = case_when(washout_start_dt == "2016-01-01" ~ 1, # exclude because joined jan 1 2016
                                                    TRUE ~ 0),
           cohort_exclusion_cal_no_washout = case_when(washout_start_dt > as.Date("2019-07-01") ~ 1,
                                                       TRUE ~ 0),  # exclude because not in study for > 6 months for washout
           cohort_exclusion_12mos_cal_no_washout = case_when(washout_start_dt > as.Date("2019-01-01") ~ 1,
                                                       TRUE ~ 0),  # exclude because not in study for > 1 year for washout
           washout_end_dt = case_when(washout_end_dt > as.Date("2019-12-31") ~ as.Date("2019-12-31"), 
                                          TRUE ~ as.Date(washout_end_dt)),
           study_end_dt = case_when(study_end_dt > as.Date("2019-12-31") ~ as.Date("2019-12-31"),
                                          TRUE ~ study_end_dt),
    ) |>
    # rename calendar cohort dates
    rename(washout_cal_end_dt = washout_end_dt,
           study_cal_end_dt = study_end_dt) |>
    full_join(dts_cont_cohort) # merge with continuous enrollment cohort

# save final cohort DF
write_parquet(dts_cohorts, "data/tafdedts/dts_cohorts.parquet")
