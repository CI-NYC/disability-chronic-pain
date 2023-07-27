################################################################################
################################################################################
###  DESCRIBE DISABILITY CODES OVER TIME (by month)
###  Kat Hoffman, June 2023
################################################################################
################################################################################

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


dis_elig_cds <- c(
    "11", # = Individuals Receiving SSI
    "12", # = Aged, Blind and Disabled Individuals in 209(b) States
    "13", # = Individuals Receiving Mandatory State Supplements
    "17", # = Individuals Who Lost Eligibility for SSI/SSP Due to an Increase in OASDI Benefits in 1972
    "18", # = Individuals Who Would be Eligible for SSI/SSP but for OASDI COLA increases since April 1977
    "19", # = Disabled Widows and Widowers Ineligible for SSI due to Increase in OASDI
    "20", # = Disabled Widows and Widowers Ineligible for SSI due to Early Receipt of Social Security
    "21", # = Working Disabled under 1619(b)
    "22", # = Disabled Adult Children
    "37", # = Aged, Blind or Disabled Individuals Eligible for but Not Receiving Cash Assistance
    "38", #  = Individuals Eligible for Cash Assistance except for Institutionalization
    "39", # = Individuals Receiving Home and Community Based Services under Institutional Rules
    "40", # = Optional State Supplement Recipients — 1634 States, and SSI Criteria States with 1616 Agreements
    "41", # = Optional State Supplement Recipients — 209(b) States, and SSI Criteria States without 1616 Agreements
    "46", # = Poverty Level Aged or Disabled
    "47", # = Work Incentives Eligibility Group
    "48", # = Ticket to Work Basic Group
    "50", # = Family Opportunity Act Children with Disabilities,
    "51", # = Individuals Eligible for Home and Community-Based Services
    "52", # = Individuals Eligible for Home and Community-Based Services — Special Income Level
    "59", # = Medically Needy Aged, Blind or Disabled
    "60"  # = Medically Needy Blind or Disabled Individuals Eligible in 1973
)

non_dis_elig_cds <- c(
    "01", # = Parents and Other Caretaker Relatives
    "02", # = Transitional Medical Assistance
    "03", # = Extended Medicaid due to Earnings
    "04", # = Extended Medicaid due to Spousal Support Collections
    "06", # = Deemed Newborns
    "07", # = Infants and Children under Age 19
    "08", # = Children with Title IV-E Adoption Assistance, Foster Care or Guardianship Care
    "09", # = Former Foster Care Children
    "14", # = Individuals Who Are Essential Spouses
    "27", # = Optional Coverage of Parents and Other Caretaker Relatives
    "28", #  = Reasonable Classifications of Individuals under Age 21
    "29", #  = Children with Non-IV-E Adoption Assistance
    "30", #  = Independent Foster Care Adolescents
    "31", #  = Optional Targeted Low Income Children
    "32", # = Individuals Electing COBRA Continuation Coverage
    "33", #  = Individuals above 133% FPL under Age 65
    "35", # = Individuals Eligible for Family Planning Services
    "49", # = Ticket to Work Medical Improvements Group
    "56", #  = Medically Needy Parents and Other Caretakers
    "61", #  = Targeted Low-Income Children
    "62", #  = Deemed Newborn
    "63", #  = Children Ineligible for Medicaid Due to Loss of Income Disregards
    "65", #  = Children with Access to Public Employee Coverage
    "67", #  = Targeted Low-Income
    "70", # = Family Planning Participants (expansion group)
    "71", # = Other expansion group
    "72", # = Adult Group - Individuals at or below 133% FPL,19-64, newly eligible for all states
    "73", # = Adult Group - Individuals at or below 133% FPL,19-64, not newly eligible for non 1905z(3) states
    "74", # = Adult Group - Individuals at or below 133% FPL,19-64, not newly eligible parent/ caretaker-relative(s) in 1905z(3) states
    "75"  # = Adult Group - Individuals at or below 133% FPL,19-64, not newly eligible nonparent/ caretaker-relative(s) in 1905z(3) states
)


# read in tafdebse data base (all years)
td <- "/home/data/12201/" # directory of interest
dbs_files <- paste0(list.files(td, pattern = "*TAFDEBSE*", recursive = TRUE)) # files of interest
dbs <- open_dataset(paste0(td, dbs_files), format="parquet", partition = "year") # arrow dataset

elig_codes_to_check <-
    dbs |>
    select(BENE_ID, RFRNC_YR, starts_with("ELGBLTY_GRP_CD")) |>
    arrange(BENE_ID, RFRNC_YR) |>
    collect()

tic()
all_elig_dis_status <-
    elig_codes_to_check |>
    rename(year = RFRNC_YR) |>
    select(-ELGBLTY_GRP_CD_LTST) |>
    pivot_longer(cols = starts_with("ELGBLTY_GRP_CD"),
                 names_to = "month",
                 values_to = "elig_code",
                 values_drop_na = T)  |>
    mutate(month = parse_number(month),
           year = as.numeric(year),
           elig_date = as.Date(paste0(year, "-", month, "-01")),
           dis_status = case_when(elig_code %in% dis_elig_cds ~ 1,
                                  elig_code %in% non_dis_elig_cds ~ 0)) |>
    select(BENE_ID, elig_date, elig_code, dis_status)
toc()

saveRDS(all_elig_dis_status, "data/tmp/all_elig_dis_status.rds")

all_elig_dis_status |> distinct(BENE_ID, dis_status) -> tmp # get distinct statuses

