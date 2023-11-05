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

set.seed(7272)

source("R/tmle_mlr3.R")

################# PRIMARY ANALYSIS

dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp.rds") 

# old <- read_rds("../data/clean/create_cohort/final/dat_lmtp.rds")

# export analytical file tables

# Denote exposure, outcome, and confounders ------------------------------------

libs <- c("glm", "lightgbm", "earth")
libs_p <- c("glm", "lightgbm")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_cal" # exposure: 4 cats of disability / pain

dat_lmtp |> count(oud_hillary_event2)
old |> count(oud_hillary_event2)


tic()
set.seed(9)
psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2,
                             super_efficient = T)
toc()

# old_res <- read_rds("projects/create_cohort/results/psi_oud_hillary_supereff.rds")

saveRDS(psi_oud_hillary, "projects/create_cohort/results/psi_oud_hillary_supereff_92623.rds")

tic()
psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                     libs, libs, libs,
                     .mlr3superlearner_folds = 2,
                     super_efficient = T)
toc()

saveRDS(psi_oud, "projects/create_cohort/results/psi_oud_supereff_92623.rds")

tic()
psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)
toc()

saveRDS(psi_oud_poison, "projects/create_cohort/results/psi_oud_poison_supereff_92623_libp.rds")


# pain as sens analysis ---------------------------------------------------

A <- "disability_pain_nomin_cal" # exposure: 4 cats of disability / pain

psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2,
                             super_efficient = T)

saveRDS(psi_oud_hillary, "projects/create_cohort/results/psi_oud_hillary_sens_pain_supereff_92623.rds")

psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                     libs, libs, libs,
                     .mlr3superlearner_folds = 2,
                     super_efficient = T)

saveRDS(psi_oud, "projects/create_cohort/results/psi_oud_sens_pain_supereff_92623.rds")

psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)

saveRDS(psi_oud_poison, "projects/create_cohort/results/psi_oud_poison_sens_pain_supereff_92623.rds")


# chronic pain 12 mos as sens analysis ---------------------------------------------------


library(mlr3)
library(mlr3learners)
library(mlr3extralearners) 
library(data.table)
library(tidyverse)
library(tictoc)

set.seed(7)

# source("R/tmle_mlr3_browser.R")
source("R/tmle_mlr3.R")

dat_lmtp <- read_rds("projects/create_cohort/data/final/dat_lmtp_sens_12mos.rds") 

libs <- c("glm", "lightgbm", "earth")
libs_p <-   c("glm",
             #  "nnet",
              "lightgbm")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_12mos_cal_1) |>
    names()
A <- "disability_pain_12mos_cal" # exposure: 4 cats of disability / pain

# 
# tic()
# psi_oud_poison <- tmle_mlr3(test_dat, A, W, "oud_poison_event2",
#                             "svm","svm","svm",
#                             .mlr3superlearner_folds = 2,
#                             super_efficient = T)
# toc()


# naive bayes is quick
# nnet and knn fail


tic()
psi_oud_poison <- tmle_mlr3(test_dat, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)
toc()

tic()
psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)
toc()

saveRDS(psi_oud_poison, "projects/create_cohort/results/psi_oud_poison_sens_12mos_supereff_92623.rds")


tic()
psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2,
                             super_efficient = T)
toc()

saveRDS(psi_oud_hillary, "projects/create_cohort/results/psi_oud_hillary_sens_12mos_supereff_92623.rds")

tic()
psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                     libs, libs, libs,
                     .mlr3superlearner_folds = 2,
                     super_efficient = T)
toc()

saveRDS(psi_oud, "projects/create_cohort/results/psi_oud_sens_12mos_supereff_92623.rds")

tic()
psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2,
                            super_efficient = T)
toc()

saveRDS(psi_oud_poison, "projects/create_cohort/results/psi_oud_poison_sens_12mos_supereff.rds")

