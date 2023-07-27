################################################################################
################################################################################
###  TMLE ANALYSES 
###  Kat Hoffman, May 2023
###  Output: saved fit objects for tmle for all 4 exposure levels, static interventions
###     run for primary, secondary analyses, and possible mediators
################################################################################
################################################################################


# devtools::install_github("nt-williams/mlr3superlearner")
# remotes::install_github("mlr-org/mlr3extralearners@*release")

library(mlr3)
library(mlr3learners)
library(mlr3extralearners) 
library(data.table)
library(tidyverse)
library(tictoc)

set.seed(7)

source("R/tmle_mlr3.R")

################# PRIMARY ANALYSIS

dat_lmtp <- read_rds("data/final/dat_lmtp.rds") 

# export analytical file tables

# Denote exposure, outcome, and confounders ------------------------------------

libs <- c("glm", "lightgbm", "earth", "glmnet")
libs_p <- c("glm", "lightgbm")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_cal" # exposure: 4 cats of disability / pain

tic()
psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud_hillary, "results/psi_oud_hillary.rds")


gc()
# 
# tic()
# psi_any_moud <- tmle_mlr3(dat_lmtp, A, W, "any_moud_event2",
#                           libs, libs, libs,
#                           .mlr3superlearner_folds = 2)
# toc()
# 
# saveRDS(psi_any_moud, "results/psi_any_moud.rds")

gc()

tic()
psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                    libs, libs, libs,
                    .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud, "results/psi_oud.rds")

tic()
psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud_poison, "results/psi_oud_poison.rds")

gc()




##### 12 month exposures


dat_lmtp <- read_rds("data/final/dat_lmtp_sens_12mos.rds") 


libs <- c("glm", "lightgbm", "earth", "glmnet")
libs_p <- c("glm", "lightgbm", "glmnet")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_12mos_cal_1) |>
    names()
A <- "disability_pain_12mos_cal" # exposure: 4 cats of disability / pain

summary(dat_lmtp[[A]])

tic()
psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud_hillary, "results/psi_oud_hillary_sens_12mos.rds")


gc()

gc()

tic()
psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                     libs, libs, libs,
                     .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud, "results/psi_oud_sens_12mos.rds")

tic()
psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud_poison, "results/psi_oud_poison_sens_12mos.rds")

gc()


tic()
psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs_p, libs_p, libs_p,
                             .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud_hillary, "results/psi_oud_hillary_sens_12mos_noearth.rds")


gc()

gc()

tic()
psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                     libs_p, libs_p, libs_p,
                     .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_oud, "results/psi_oud_sens_12mos_noearth.rds")



##### Rerun with any pain as an exposure

dat_lmtp <- read_rds("data/final/dat_lmtp.rds") 

dat_lmtp |> count(disability_pain_nomin_cal)

libs <- c("glm", "lightgbm", "earth", "glmnet")
libs_p <- c("glm", "lightgbm")

lib_test <- "glmnet"

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_nomin_cal" # exposure: 4 cats of disability / pain

psi_oud_hillary <- tmle_mlr3(dat_lmtp, A, W, "oud_hillary_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2)

saveRDS(psi_oud_hillary, "results/psi_oud_hillary_sens_pain.rds")

psi_oud <- tmle_mlr3(dat_lmtp, A, W, "oud_event2", 
                     libs, libs, libs,
                     .mlr3superlearner_folds = 2)

saveRDS(psi_oud, "results/psi_oud_sens_pain.rds")

psi_oud_poison <- tmle_mlr3(dat_lmtp, A, W, "oud_poison_event2",
                            libs_p, libs_p, libs_p,
                            .mlr3superlearner_folds = 2)

saveRDS(psi_oud_poison, "results/psi_oud_poison_sens_pain.rds")



#############################################
##### Rerun within no chronic pain subset
#############################################

dat_lmtp <- dat_lmtp |>
    filter(disability_pain_cal %in% c(3,4))

libs <- c("glm", "lightgbm", "earth", "glmnet")
libs_p <- c("glm", "lightgbm")

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_cal" # exposure: 4 cats of disability / pain
dat_lmtp[[A]] <- fct_drop(dat_lmtp[[A]] )

# CHRONIC PAIN
tic()
# already filtered out chronic pain from exposure, don't need to do any additional filtering
psi_cp <- tmle_mlr3(dat_lmtp, A, W, "chronic_pain_event2",
                             libs, libs, libs,
                             .mlr3superlearner_folds = 2)
saveRDS(psi_cp, "results/psi_cp.rds")
toc()

# Anxiety

dat_anxiety <- dat_lmtp |> filter(anxiety_washout_cal_1 == 0)
# filter out those with baseline anxiety, don't adjust for it
W <- W[-which(W=="anxiety_washout_cal_1")]
tic()
psi_anxiety <- tmle_mlr3(dat_anxiety, A, W, "anxiety_event2",
                    libs, libs, libs,
                    .mlr3superlearner_folds = 2)
saveRDS(psi_anxiety, "results/psi_anxiety.rds")
toc()


# Depression
dat_depression <- dat_lmtp |> filter(depression_washout_cal_1 == 0)
# filter out those with baseline anxiety, don't adjust for it
W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
W <- W[-which(W=="depression_washout_cal_1")]
tic()
psi_depression <- tmle_mlr3(dat_depression, A, W, "depression_event2",
                         libs, libs, libs,
                         .mlr3superlearner_folds = 2)
saveRDS(psi_depression, "results/psi_depression.rds")
toc()

# opioid prescriptions for pain
dat_opioids <- read_rds("data/final/analysis_cohort.rds")  |> select(BENE_ID, opioid_pain_washout_cal)

# add opioid prescriptions for pain back into data
dat_opioids <- dat_lmtp |> 
    left_join(dat_opioids) |>
    filter(opioid_pain_washout_cal == 0)

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
tic()
psi_opioids <- tmle_mlr3(dat_opioids, A, W, "depression_event2",
                            libs, libs, libs,
                            .mlr3superlearner_folds = 2)
saveRDS(psi_opioids, "results/psi_opioids.rds")
toc()


#### Continuous outcomes

# install.packages("scripts/09_adjusted_analysis/mlr3superlearner_0.1.0.tar.gz", type="source")

library(mlr3)
library(mlr3learners)
library(mlr3extralearners) # remotes::install_github("mlr-org/mlr3extralearners@*release")
library(data.table)
library(tidyverse)
library(tictoc)

set.seed(7)

source("R/tmle_mlr3_browser.R")

dat_lmtp <- read_rds("data/final/dat_lmtp.rds") 

# export analytical file tables

# Denote exposure, outcome, and confounders ------------------------------------

libs <- c("glm", "lightgbm", "earth", "glmnet")
libs_p <- c("glm", "lightgbm")

extra_var <-
    read_rds("data/final/analysis_cohort.rds")  |> 
    select(BENE_ID, oud_misuse_cal_avg_total_days_supply) |>
    mutate(oud_misuse_cal_avg_total_days_supply = replace_na(oud_misuse_cal_avg_total_days_supply, 0))

# merge days_supply variable into dat_lmtp
dat_lmtp_days <-
    dat_lmtp |>
    left_join(extra_var)

W <-  dat_lmtp_days |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()
A <- "disability_pain_cal" # exposure: 4 cats of disability / pain

tic()
psi_days <- tmle_mlr3(dat_lmtp_days, A, W, "oud_misuse_cal_avg_total_days_supply",
                 "glm","glm","glm",
                 "continuous", .mlr3superlearner_folds = 2)
toc()

saveRDS(psi_days, "results/psi_days.rds")

