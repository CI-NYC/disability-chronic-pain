################################################################################
################################################################################
###  TMLE MLR3 ANALYSIS EXAMPLES 
###  Author Nick Williams, May 2023
###  Purpose: show examples of analysis using tmle_mlr3.R script
###  Output: fit object with psi, standard error, g, Q, influence curve estimates
################################################################################
################################################################################

# remotes::install_github("nt-williams/mlr3superlearner")

library(data.table)
library(mlr3extralearners)

source("R/tmle_mlr3.R")

n <- 1e6
W <- matrix(rnorm(n*3), ncol = 3)
Y.1 <- rbinom(n, 1, plogis(1 + 0.2*W[,1] + 0.1*W[,2] + 0.2*W[,3]^2 ))
Y.0 <- rbinom(n, 1, plogis(0.2*W[,1] + 0.1*W[,2] + 0.2*W[,3]^2 ))

# binomial
n <- 1e4
W <- matrix(rnorm(n*3), ncol = 3)
A <- rbinom(n, 1, 1 / (1 + exp(-(.2*W[,1] - .1*W[,2] + .4*W[,3]))))
Y <- rbinom(n, 1, plogis(A + 0.2*W[,1] + 0.1*W[,2] + 0.2*W[,3]^2 ))
obs <- rbinom(n, 1, 1 / (1 + exp(.2*W[,1] - .1*W[,2])))

tmp <- data.frame(W, A, Y)
tmp$A <- as.factor(tmp$A)
tmp$Y <- ifelse(obs == 0, NA_real_, tmp$Y)

psi <- tmle_mlr3(tmp, "A", c("X1", "X2", "X3"), "Y",
                 c("glm", "lightgbm", "ranger"),
                 c("glm", "lightgbm", "ranger"),
                 c("glm", "lightgbm", "ranger"),
                 .mlr3superlearner_folds = 5, 
                 super_efficient = T)
gc()

# continuous
n <- 1e3
W <- matrix(rnorm(n*3), ncol = 3)
A <- rbinom(n, 1, 1 / (1 + exp(-(.2*W[,1] - .1*W[,2] + .4*W[,3]))))
A_cat <- sample(1:4, size=n, replace=T)
Y <- rnorm(n, A + 0.2*W[,1] + 0.1*W[,2] + 0.2*W[,3]^2)
obs <- rbinom(n, 1, 1 / (1 + exp(.2*W[,1] - .1*W[,2])))

tmp <- data.frame(W, A, Y, A_cat)
tmp$A <- as.factor(tmp$A)
tmp$A_cat <- as.factor(tmp$A_cat)
tmp$Y <- ifelse(obs == 0, NA_real_, tmp$Y)

psi <- tmle_mlr3(tmp, "A", c("X1", "X2", "X3"), "Y",
                 c("glm", "lightgbm", "ranger"),
                 c("glm", "lightgbm", "earth"),
                 c("glm", "lightgbm", "ranger"),
                 "continuous", 5)

psi <- tmle_mlr3(tmp, "A_cat", c("X1", "X2", "X3"), "Y",
                 c("glm", "lightgbm", "ranger"),
                 c("glm", "lightgbm", "earth"),
                 c("glm", "lightgbm", "ranger"),
                 "continuous", 5)
gc()
