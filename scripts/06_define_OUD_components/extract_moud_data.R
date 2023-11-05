################################################################################
################################################################################
###  ADD MOUD Prescriptions (any)
###  Kat Hoffman, Mar 2023
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

# source("scripts/ICD_codes/OUD.R")

# Load necessary files ---------------------------------------------------------

dts_cohorts <- open_dataset("data/tafdedts/dts_cohorts.parquet") |>
    collect() |> 
    mutate(index = rep(1:32, length.out=n()))

moud_ndcs <- read_csv("input/NDC_codes/moud_ndcs.csv")

#ndc_list_clean <- read_rds( "input/NDC_codes/ndc_list_clean.rds")
#bup_ndc_dose_clean <- read_rds("input/NDC_codes/bup_ndc_dose_clean.rds")

# Functions ---------------------------------------------------------

# Read in data files ---------------------------------------------------------

td <- "/home/data/12201/" # directory of interest

# Read in RXL (pharmacy line)
files <- paste0(list.files(td, pattern = "TAFRXL", recursive = TRUE))
rxl <- open_dataset(paste0(td, files), format="parquet")

# read in OTL (other services line)
files <- paste0(list.files(td, pattern = "TAFOTL", recursive = TRUE))
otl <- open_dataset(paste0(td, files), format="parquet")

# Filter arrow files by cohort ---------------------------------------------------------

oud_moud_rxl <-
    rxl |>
    filter(NDC %in% moud_ndcs$ndc) |>
    select(BENE_ID, 
           NDC,
           CLM_ID,
           NDC_UOM_CD, 
           NDC_QTY,
           DAYS_SUPPLY,
           RX_FILL_DT) |>
    collect() |>
    left_join(moud_ndcs |> rename(NDC = ndc, moud = moud_med))
        
# extract Nal injection scripts
nal_scripts_rxl <- 
    oud_moud_rxl |>
    filter(moud == "nal") |>
    select(BENE_ID,
           # CLM_ID,
           moud,
           RX_FILL_DT) |>
    distinct() 

# filter BUP scripts, merge BUP NDC-dose map to compute strength/day
bup_scripts_rxl_all <-
    oud_moud_rxl |>
    filter(moud == "bup") |>
    select(BENE_ID,
           CLM_ID,
           moud,
           NDC,
           NDC_UOM_CD, 
           NDC_QTY,
           DAYS_SUPPLY,
           RX_FILL_DT,
           check,
           strength_clean
    ) |>
    collect() |>
    mutate(pills_per_day = NDC_QTY/DAYS_SUPPLY,
           strength_per_day = strength_clean * pills_per_day)

# only keep relevant strengths/day of BUP scripts
bup_scripts_rxl <-
    bup_scripts_rxl_all |>
    mutate(keep = case_when(check == 0 ~ 1, # keep  if don't need to check
                            check == 1 & strength_per_day >= 10 ~ 1, # less than 10 counts towards probable misuse (and chronic pain)
                            check == 1 & strength_per_day < 50 ~ 1,
                            TRUE ~ 0
                            )) |>
    filter(keep == 1) |>
    # select(BENE_ID, RX_FILL_DT, moud) |>
    distinct()

write_rds(nal_scripts_rxl, "data/oud_info/nal_scripts_rxl.rds")
write_rds(bup_scripts_rxl, "data/oud_info/bup_scripts_rxl.rds")

# Extract all of the scripts (NDC codes) in Other Services
tic()
moud_scripts_otl <-
    otl |>
    filter(NDC %in% moud_ndcs$ndc) |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT)) |>
    select(BENE_ID,
           CLM_ID,
           NDC,
           NDC_UOM_CD, 
           NDC_QTY,
           LINE_SRVC_BGN_DT,
           LINE_SRVC_END_DT,
           LINE_PRCDR_CD,
           LINE_PRCDR_CD_SYS,
           ACTL_SRVC_QTY,
           ALOWD_SRVC_QTY)  |>
    collect() |>
    left_join(moud_ndcs |> rename(NDC = ndc))
toc()

# save the bup injections (definitely MOUD)
bup_injections_otl <- 
    moud_scripts_otl |>
    filter(moud_med == "bup") |>
    filter(str_detect(dosage_form, "INJECTION"))

saveRDS(bup_injections_otl, file = "data/oud_info/bup_injections_otl.rds")

bup_check_otl <- 
    moud_scripts_otl |>
    filter(moud_med == "bup") |>
    filter(str_detect(dosage_form, "TABLET|FILM"), check == 1) 

bup_tablets_otl <-
    bup_check_otl |>
    mutate(strength_times_quantity = case_when(NDC_UOM_CD == "UN" ~ strength_clean * NDC_QTY, TRUE ~ strength_clean)) |>
    group_by(BENE_ID, LINE_SRVC_BGN_DT) |>
    add_count() |>
    summarize(strength_per_day = sum(strength_times_quantity))

saveRDS(bup_tablets_otl, file = "data/oud_info/bup_tablets_otl.rds")

bup_otl_over10mg <-
    bup_tablets_otl |>
    filter(strength_per_day >= 10)

saveRDS(bup_otl_over10mg, file = "data/oud_info/bup_otl_over10mg.rds")

bup_naloxone_otl <- 
    moud_scripts_otl |>
    filter(moud_med == "bup") |>
    filter(str_detect(dosage_form, "TABLET|FILM"), check == 0)  # these are bup + naloxone, calling moud

saveRDS(bup_naloxone_otl, file = "data/oud_info/bup_naloxone_otl.rds")

# nal injections
nal_scripts_otl <-
    moud_scripts_otl |>
    filter(moud_med == "nal") |>
    collect() 

# next step: filter other services for procedure codes

# define HCPCS codes - https://docs.google.com/spreadsheets/d/1S5SuyHEugP-Zc022H-DvIR8Nl6NxqxYCBRlk_GgAH7c/edit#gid=132338121

met_hcpcs <- c("H0020",
               # "S0109", # see later criteria (IA, 2016)
               "J1230", # fixed July 2023
               "HZ81ZZZ3", 
               "HZ91ZZZ3"
)

bup_hcpcs <- c(
    "J0570",
    # "J0571",
    "J0572",
    "J0573",
    "J0574",
    "J0575",
    "Q9991",
    "Q9992")

nal_hcpcs <- "J2315"


moud_hcpcs <-
    otl |>
    mutate(LINE_SRVC_BGN_DT = case_when(is.na(LINE_SRVC_BGN_DT) ~ LINE_SRVC_END_DT, TRUE ~ LINE_SRVC_BGN_DT),
           year = year(LINE_SRVC_BGN_DT),
           moud = case_when(LINE_PRCDR_CD == "S0109" ~ "met check",
                                LINE_PRCDR_CD %in% met_hcpcs ~ "met",
                                LINE_PRCDR_CD %in% bup_hcpcs ~ "bup",
                                LINE_PRCDR_CD %in% nal_hcpcs ~ "nal"
    )
    ) |>
    filter(!is.na(moud)) |>
    select(BENE_ID,
           STATE_CD, 
           year,
           moud,
           NDC,
           NDC_UOM_CD, 
           NDC_QTY,
           LINE_SRVC_BGN_DT,
           LINE_SRVC_END_DT,
           LINE_PRCDR_CD,
           LINE_PRCDR_CD_SYS,
           ACTL_SRVC_QTY,
           ALOWD_SRVC_QTY) 

# all relevant MOUD medications using procedure codes
tic()
all_rel_hcpcs <-
    moud_hcpcs |>
    collect() |>
    mutate(moud = case_when(moud == "met check" & STATE_CD == "IA" & year == "2016" ~ "met",
                                moud == "met check" ~ NA_character_, # if "S0109" but not part of the IA 2016 criteria, remove
                                TRUE ~ moud)
           ) |>
    drop_na(moud)   
toc()

tic()
saveRDS(nal_scripts_otl, "data/oud_info/nal_scripts_otl.rds")
saveRDS(all_rel_hcpcs, "data/oud_info/all_rel_hcpcs_otl.rds")
toc()

### next steps:
# 3. Make a "probable detox" variable

