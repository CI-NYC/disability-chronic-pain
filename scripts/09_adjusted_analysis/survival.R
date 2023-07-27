################################################################################
################################################################################
###  TIME TO EVENT ANALYSIS
###  Kat Hoffman, May 2023
###  Output: saved fit objects for LMTP mlr3 version run with survival outcomes 
###  and for all 4 static epxosure level interventions
################################################################################
################################################################################

# remotes::install_github("nt-williams/mlr3superlearner")
# remotes::install_github("nt-williams/lmtp@mlr3superlearner")
# remotes::install_github("mlr-org/mlr3extralearners")

library(lmtp)
library(tictoc)
library(tidyverse)
library(lubridate)
library(mlr3)
library(mlr3learners)
library(mlr3extralearners) 
options(future.globals.maxSize = 8000 * 1024^2)

dat_lmtp <- read_rds("data/final/dat_lmtp.rds")
analysis_cohort <- read_rds("data/final/analysis_cohort.rds")


# clean survival data
surv_dts <- analysis_cohort |>
    select(BENE_ID, washout_start_dt, oud_hillary_dt, oud_hillary_study_cal, study_cal_end_dt, censoring_ever_dt) |>
    mutate(days_to_oud_hillary = case_when(oud_hillary_study_cal == 1 ~ difftime(oud_hillary_dt,  washout_start_dt, units = "days"),
                                           !is.na(censoring_ever_dt) ~ difftime(censoring_ever_dt,  washout_start_dt, units = "days"),
                                           TRUE ~ difftime(study_cal_end_dt, washout_start_dt, units = "days")
    ),
    censored = case_when(is.na(oud_hillary_dt) & !is.na(censoring_ever_dt) ~ 1, TRUE ~ 0)
    )

names(dat_lmtp)

dat_lmtp_surv <- dat_lmtp |>
    left_join(surv_dts)

libs <- c("glm", "lightgbm")

# make 3 outcomes (6-12, 12-18, 18-24 months)
dat_lmtp_surv_clean <-
    dat_lmtp_surv |>
    mutate(Y_1 = case_when(oud_hillary_study_cal==1 & days_to_oud_hillary < 365 ~ 1,
                           censored == T & days_to_oud_hillary < 365 ~ NA,
                           TRUE ~ 0),
           Y_2 = case_when(oud_hillary_study_cal==1 & days_to_oud_hillary < 547 ~ 1,
                           censored == T & days_to_oud_hillary < 547 ~ NA,
                           TRUE ~ 0),
           Y_3 = case_when(oud_hillary_study_cal==1 & days_to_oud_hillary < 730 ~ 1,
                           censored == T & days_to_oud_hillary < 730 ~ NA,
                           TRUE ~ 0),
           C_1 = case_when(Y_1 == 1 ~ 1,
                           is.na(Y_1) ~ 0,
                           TRUE ~ 1),
           C_2 = case_when(Y_2 == 1 ~ 1,
                           is.na(Y_2) ~ 0,
                           TRUE ~ 1),
           C_3 = case_when(Y_3 == 1 ~ 1,
                           is.na(Y_3) ~ 0,
                           TRUE ~ 1)) 

# create shifted data set
shift_1 <- dat_lmtp_surv_clean |> mutate(disability_pain_cal = factor(1, levels=1:4),
                                            C_1 = 1,
                                            C_2 = 1,
                                            C_3 = 1)

shift_2 <- dat_lmtp_surv_clean |> mutate(disability_pain_cal = factor(2, levels=1:4),
                                         C_1 = 1,
                                         C_2 = 1,
                                         C_3 = 1)

shift_3 <- dat_lmtp_surv_clean |> mutate(disability_pain_cal = factor(3, levels=1:4),
                                         C_1 = 1,
                                         C_2 = 1,
                                         C_3 = 1)

shift_4 <- dat_lmtp_surv_clean |> mutate(disability_pain_cal = factor(4, levels=1:4),
                                         C_1 = 1,
                                         C_2 = 1,
                                         C_3 = 1)

Y <- c("Y_1", "Y_2", "Y_3")
C <- c("C_1", "C_2", "C_3")
A <- "disability_pain_cal"

W <-  dat_lmtp |> # all confounder vars are together from data cleaning
    select(dem_age:mental_ill_washout_cal_1) |>
    names()

# c_1 <- function(data, trt){
#     trt <- 1
#     return(factor(data[[trt]], levels = 1:4)) # return the refactored treatment level
# }
# 
# c_2 <- function(data, trt){
#     trt <- 2
#     return(factor(data[[trt]], levels = 1:4)) # return the refactored treatment level
# }
# 
# c_3 <- function(data, trt){
#     trt <- 3
#     return(factor(data[[trt]], levels = 1:4)) # return the refactored treatment level
# }
# 
# c_4 <- function(data, trt){
#     trt <- 2
#     return(factor(data[[trt]], levels = 1:4)) # return the refactored treatment level
# }
# 
# test_cohort <- dat_lmtp_surv_clean |>
#     group_by(disability_pain_cal) |>
#     sample_n(5000) |>
#     ungroup()
# test_libs <- c("glm", "lightgbm")
# 
# shift_1 <- test_cohort |> mutate(disability_pain_cal = factor(1, levels=1:4),
#                                          C_1 = 1,
#                                          C_2 = 1,
#                                          C_3 = 1)

# ?lmtp_control .learners_trt_metalearner

# note that metalearner = glm
# tic()
# test_fit <-
#     progressr::with_progress({
#         lmtp_tmle(test_cohort,
#                   trt = A,
#                   outcome = Y,
#                   baseline = W,
#                   cens = C,
#                   folds = 1,
#                   learners_outcome = test_libs,
#                   learners_trt = test_libs,
#                   outcome_type = "survival",
#                   shifted = shift_1, 
#                   control = lmtp_control(.learners_trt_folds = 2, .learners_outcome_folds = 2)
#         )
#     }
#     )
# toc()

tic()
fit1 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  learners_outcome = libs,
                  learners_trt = libs,
                  outcome_type = "survival",
                  shifted = shift_1, 
                  control = lmtp_control(.learners_trt_folds = 2, .learners_outcome_folds = 2)
        )
    }
    )
toc()

saveRDS(fit1, "results/tte_fit1.rds")



fit2 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  learners_outcome = libs,
                  learners_trt = libs,
                  outcome_type = "survival",
                  shifted = shift_2, 
                  control = lmtp_control(.learners_trt_folds = 2, .learners_outcome_folds = 2)
        )
    }
    )


saveRDS(fit2, "results/tte_fit2.rds")


fit3 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  learners_outcome = libs,
                  learners_trt = libs,
                  outcome_type = "survival",
                  shifted = shift_3, 
                  control = lmtp_control(.learners_trt_folds = 2, .learners_outcome_folds = 2)
        )
    }
    )

saveRDS(fit3, "results/tte_fit3.rds")


fit4 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  learners_outcome = libs,
                  learners_trt = libs,
                  outcome_type = "survival",
                  shifted = shift_4, 
                  control = lmtp_control(.learners_trt_folds = 2, .learners_outcome_folds = 2)
        )
    }
    )

tic()

saveRDS(fit4, "results/tte_fit4.rds")





# glm only ----------------------------------------------------------------




tic()
fit1 <-
    progressr::with_progress({
    lmtp_tmle(dat_lmtp_surv_clean,
              trt = A,
              outcome = Y,
              baseline = W,
              cens = C,
              folds = 1,
             .learners_trt_folds = 2,
             .learners_outcome_folds = 2,
              outcome_type = "survival",
              # shift = c_1,
             shifted = shift_1
             )
    }
    )
toc()

saveRDS(fit1, "results/tte_fit1_glm.rds")
             

fit2 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  .learners_trt_folds = 2,
                  .learners_outcome_folds = 2,
                  outcome_type = "survival",
                  # shift = c_1,
                  shifted = shift_2
        )
    }
    )

saveRDS(fit2, "results/tte_fit2_glm.rds")


fit3 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  .learners_trt_folds = 2,
                  .learners_outcome_folds = 2,
                  outcome_type = "survival",
                  # shift = c_1,
                  shifted = shift_3
        )
    }
    )

saveRDS(fit3, "results/tte_fit3_glm.rds")


fit4 <-
    progressr::with_progress({
        lmtp_tmle(dat_lmtp_surv_clean,
                  trt = A,
                  outcome = Y,
                  baseline = W,
                  cens = C,
                  folds = 1,
                  .learners_trt_folds = 2,
                  .learners_outcome_folds = 2,
                  outcome_type = "survival",
                  # shift = c_1,
                  shifted = shift_4
        )
    }
    )

tic()

saveRDS(fit4, "results/tte_fit4_glm.rds")

