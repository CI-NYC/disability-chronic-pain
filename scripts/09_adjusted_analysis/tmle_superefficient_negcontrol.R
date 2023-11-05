################################################################################
################################################################################
###  TMLE ANALYSES - superefficient estimator
###  Kat Hoffman, May 2023
###  Output: saved fit objects for tmle using superefficient estimator
################################################################################
################################################################################

# devtools::install_github("nt-williams/mlr3superlearner@devel")

library(mlr3)
library(mlr3learners)
library(mlr3extralearners) 
library(data.table)
library(tidyverse)
library(tictoc)

set.seed(7)

source("R/tmle_mlr3.R")

# dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp_asph.rds") 

# dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp_ear_infection.rds") 
# psi <- read_rds("projects/create_cohort/results/psi_negcontrol_ear_infection.rds")
dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp_negcontrol.rds") 

# export analytical file tables

# Denote exposure, outcome, and confounders ------------------------------------

# libs <- c("glm", "lightgbm", "earth")
libs <- c("glm", "lightgbm")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_cal" # exposure: 4 cats of disability / pain


tic()
psi_negcontrol <- tmle_mlr3(dat_lmtp, A, W, "asph_event",
                            libs, libs, libs,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)
toc()

saveRDS(psi_negcontrol, "projects/create_cohort/results/psi_negcontrol_asph.rds")

tic()
psi_negcontrol <- tmle_mlr3(dat_lmtp, A, W, "ear_event",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2,
                             super_efficient = T)
toc()

psi_negcontrol$psi

saveRDS(psi_negcontrol, "projects/create_cohort/results/psi_negcontrol_ear.rds")



tic()
psi_negcontrol <- tmle_mlr3(dat_lmtp, A, W, "app_event",
                            libs, libs, libs,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)
toc()

saveRDS(psi_negcontrol, "projects/create_cohort/results/psi_negcontrol_app.rds")


# negative control for exposure -------------------------------------------

dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp_negcontrol_exposure.rds") |>
    mutate(appendix_washout = factor(case_when(appendix_washout==1 ~ "yes", TRUE ~ "no")))


# libs <- c("glm", "lightgbm", "earth")
libs <- c("glm", "lightgbm")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1, disability_pain_cal) |>
    names()
A <- "appendix_washout" # exposure: 4 cats of disability / pain


tic()
set.seed(9)
psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2,
                             super_efficient = T)
toc()

saveRDS(psi_oud_hillary, "projects/create_cohort/results/psi_negcontrol_app_exp.rds")


