################################################################################
################################################################################
###  TMLE FOR CONTINUOUS OUTCOME (days supply)
###  Author Kat Hoffman, May 2023
###  Output: fit object with psi, standard error, g, Q, influence curve estimates
###     for outcome of days supply
################################################################################
################################################################################

library(mlr3)
library(mlr3learners)
library(mlr3extralearners) 
library(data.table)
library(tidyverse)
library(tictoc)

set.seed(7)

source("R/tmle_mlr3.R")

dat_lmtp_days <- read_rds("data/final/dat_lmtp_days_supply.rds") 

libs <- c("glm", "lightgbm", "earth", "glmnet") 

W <-  dat_lmtp_days |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_cal" # exposure: 4 cats of disability / pain

Y <- "oud_misuse_cal_avg_total_days_supply" # avg days supply of opioid presc.

tic()
psi_days <- tmle_mlr3(dat_lmtp_days, A, W, Y,
                      libs, libs, libs,
                      "continuous",
                      .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_days, "results/psi_days.rds")

rm(psi_days)

libs <- c("glm", "lightgbm", "earth") 

tic()
psi_days <- tmle_mlr3(dat_lmtp_days, A, W, Y,
                      libs, libs, libs,
                      "continuous",
                      .mlr3superlearner_folds = 2,
                      super_efficient = T)
toc()

saveRDS(psi_days, "results/psi_days_supereff.rds")

