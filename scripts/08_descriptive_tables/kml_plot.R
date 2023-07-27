################################################################################
################################################################################
###  CREATE A KAPLAN MEIER PLOT 
###  Kat Hoffman, March 2023
###  Output: KM plot (not used for paper)
################################################################################
################################################################################

library(tidylog)
library(tidyverse)
library(janitor)
library(arrow)
library(gtsummary)
library(gt)
library(tictoc)
library(survminer)
library(lubridate)


analysis_cohort <-  read_rds("data/final/analysis_cohort.rds") |>
    mutate(disability_pain_cal = fct_relevel(disability_pain_cal, "disability and chronic pain", "chronic pain only"))

km_dat <- analysis_cohort |>
    select(BENE_ID, disability_pain_cal, washout_start_dt, oud_hillary_dt, censoring_ever_dt) |>
    mutate(fu = case_when(!is.na(oud_hillary_dt) ~ as.numeric(difftime(oud_hillary_dt, washout_start_dt, units="days")),
                                        !is.na(censoring_ever_dt) ~ as.numeric(difftime(censoring_ever_dt, washout_start_dt, units= "days")),
                                        TRUE ~ 730),
           status = case_when(!is.na(oud_hillary_dt) ~ 1,
                              TRUE ~ 0
           )) |>
    select(disability_pain_cal, fu, status)

fit <- survfit(Surv(fu, status) ~ disability_pain_cal, data = km_dat)

p <- ggsurvplot(fit, data = km_dat)


# 

kmfit <- read_rds("projects/create_cohort/output/fit.rds")
plot(kmfit, xlim=c(0,730), col = c("black", "grey", "blue","red"))

