################################################################################
################################################################################
###  DESCRIBE DISABILITY CODES (ALL CALENDAR YEAR STUDY DURATION)
###  Kat Hoffman, June 2023
################################################################################
################################################################################

library(arrow)
library(tidyverse)
library(lubridate)
library(data.table)
library(furrr)
library(tidylog)
library(tictoc)

analysis_cohort <- read_rds("data/final/analysis_cohort.rds")

dis_cohort <-
    analysis_cohort |>
    filter(disability_washout_cal == 1) |>
    select(BENE_ID, washout_start_dt, study_cal_end_dt)

# read in tafihp data base (all years)
td <- "/home/data/12201/" # directory of interest
iph_files <- paste0(list.files(td, pattern = "*TAFIPH*", recursive = TRUE)) # files of interest
iph <- open_dataset(paste0(td, iph_files), format="parquet") # arrow dataset

# Other services
td <- "/home/data/12201/" # directory of interest
oth_files <- paste0(list.files(td, pattern = "*TAFOTH*", recursive = TRUE)) # files of interest
oth <- open_dataset(paste0(td, oth_files), format="parquet") # arrow dataset

icd_codes_to_check <-
    iph |>
    select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) |>
    collect()

# obtain the date for all inpatient hospitalization services within hospital period
all_iph_icds_in_washout_cal <-
    icd_codes_to_check |>
    inner_join(dis_cohort) |> # merge dsiabilty cohort
    mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    filter(SRVC_BGN_DT %within% interval(washout_start_dt, study_cal_end_dt))

dgcd_long_iph <-
    all_iph_icds_in_washout_cal |>
    collect() |>
    pivot_longer(starts_with("DGNS_CD"), names_to = "tmp_col", values_to = "code") |>
    select(-tmp_col) |>
    drop_na(code) |>
    select(BENE_ID, code) |>
    distinct()

oth_query <- oth |>
    filter(BENE_ID %in% dis_cohort$BENE_ID) |>
    mutate(SRVC_BGN_DT = case_when(is.na(SRVC_BGN_DT) ~ SRVC_END_DT, TRUE ~ SRVC_BGN_DT)) |>
    select(BENE_ID, SRVC_BGN_DT, SRVC_END_DT, contains("DGNS_CD")) 


tic()
dgcd_join <- 
    oth_query |>
    collect() |>
    left_join(dis_cohort) |>
    filter(SRVC_BGN_DT %within% interval(washout_start_dt, study_cal_end_dt))

dgcd_long_oth <- 
    dgcd_join |>
    distinct(BENE_ID, DGNS_CD_1, DGNS_CD_2) |>
    pivot_longer(cols = DGNS_CD_1:DGNS_CD_2, names_to = "tmp_col", values_to = "code") |>
    select(-tmp_col) |>
    drop_na(code) |>
    distinct()
toc()

dgcd_long_24mos <-
    bind_rows(dgcd_long_iph, dgcd_long_oth) |>
    distinct()

write_rds(dgcd_long_24mos, "data/tmp/dgcd_long_24mos.rds")

dgcd_long_24mos <-  read_rds("data/tmp/dgcd_long_24mos.rds")

dgcd_explain <- icd::explain_table(dgcd_long_24mos$code) |>
    select(short_desc, long_desc, major, sub_chapter, chapter) 

dgcd_explain <-
    dgcd_long_24mos |>
    bind_cols(dgcd_explain) |>
    select(BENE_ID, code, short_desc, long_desc, major, sub_chapter, chapter)

acute_cds <-
    dgcd_explain |>
    filter_at(vars(short_desc, long_desc, major, sub_chapter), any_vars(str_detect(., "acute|Acute"))) |>
    filter_at(vars(short_desc, long_desc, major, sub_chapter), any_vars(!str_detect(., "kidney|Kidney")))  |>
    distinct(code)|>
    pull(code)

labs_cds <-
    dgcd_explain |>
    filter(str_detect(chapter, "Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified"))|>
    distinct(code)|>
    pull(code)

injury_cds <-
    dgcd_explain |>
    filter(str_detect(sub_chapter, "Injury|injury|Injuries|injuries|Acute Kidney Failure And Chronic Kidney Disease")) |>
    distinct(code)|>
    pull(code)

copd_cereb_cds <-
    dgcd_explain |>
    filter(str_detect(major, "Cerebral infarction|chronic obstructive pulmonary disease")) |>
    distinct(code) |>
    pull(code)

ok_cds <-
    dgcd_explain |>
    filter(!(code %in% c(acute_cds, labs_cds)))

ok_cd_ids <- distinct(ok_cds, BENE_ID)

c1 <- "Diseases of the musculoskeletal system and connective tissue"
c2 <- "Mental, Behavioral and Neurodevelopmental disorders"
c3 <-  "Diseases of the respiratory system"
c4 <-  "Diseases of the circulatory system"

n1 <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter == c1) |>
    distinct(BENE_ID) |>
    nrow()

n2 <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter == c2) |>
    distinct(BENE_ID)|>
    nrow() # 

n3 <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter == c3) |>
    distinct(BENE_ID)|>
    nrow() # 

n4 <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter == c4) |>
    distinct(BENE_ID)|>
    nrow() # 

n5 <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter %in% c(c1, c2, c3, c4)) |>
    distinct(BENE_ID)|>
    nrow() # 

n6<- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter %in% c(c1, c3, c4)) |>
    distinct(BENE_ID)|>
    nrow() # 

n7 <-  dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(str_detect(major, "Cerebral infarction")) |>
    distinct(BENE_ID)|>
    nrow() # 

n8 <-  dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(str_detect(sub_chapter, "Injury|injury|Injuries|injuries|Acute Kidney Failure And Chronic Kidney Disease")) |>
    distinct(BENE_ID)|>
    nrow() # 

# have no codes
ids_no_code <- dis_cohort |>
    filter(!(BENE_ID %in% dgcd_long_24mos$BENE_ID)) 

# denominator: don't have any acute, non kidney related issues

musc_ids <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(chapter == c1) |>
    distinct(BENE_ID) 

inj_ids <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(!(BENE_ID %in% musc_ids$BENE_ID)) |>
    filter(str_detect(sub_chapter, "Injury|injury|Injuries|injuries|Acute Kidney Failure And Chronic Kidney Disease")) |>
    distinct(BENE_ID)

kidney_ids <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(!(BENE_ID %in% c(musc_ids$BENE_ID, inj_ids$BENE_ID))) |>
    filter_at(vars(short_desc, long_desc, major, sub_chapter), any_vars(str_detect(., "kidney|Kidney")))  |>
    distinct(BENE_ID)

stroke_ids <- dgcd_explain |>
               filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
               filter(!(BENE_ID %in% c(musc_ids$BENE_ID, inj_ids$BENE_ID, kidney_ids$BENE_ID))) |>
               filter(str_detect(major, "Cerebral infarction")) |>
               distinct(BENE_ID)
           
ment_ids <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(!(BENE_ID %in% c(musc_ids$BENE_ID, inj_ids$BENE_ID, kidney_ids$BENE_ID, stroke_ids$BENE_ID))) |>
    filter(chapter == c2) |>
    distinct(BENE_ID)

resp_ids <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(!(BENE_ID %in% c(musc_ids$BENE_ID, inj_ids$BENE_ID, kidney_ids$BENE_ID, stroke_ids$BENE_ID, ment_ids$BENE_ID))) |>
    filter(chapter == c3) |>
    distinct(BENE_ID)

circ_ids <- dgcd_explain |>
    filter(BENE_ID %in% ok_cd_ids$BENE_ID) |>
    filter(!(BENE_ID %in% c(musc_ids$BENE_ID, inj_ids$BENE_ID, kidney_ids$BENE_ID, stroke_ids$BENE_ID, ment_ids$BENE_ID, resp_ids$BENE_ID))) |>
    filter(chapter == c4) |>
    distinct(BENE_ID)

 ids <- list(musc_ids, inj_ids, kidney_ids, stroke_ids, ment_ids, resp_ids, circ_ids)
 names(ids) <- c("musc","inj","kid","stroke","ment","resp","circ")
 map(ids, ~nrow(.x)/nrow(ok_cd_ids)*100)
 
