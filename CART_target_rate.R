library(dplyr)
library(readr)
library(rpart)
library(rpart.plot)
library(RColorBrewer)
library(tidyverse)
library(stringr)
library(lubridate)
library(mlr)
library(bigrquery)
library(ROCit)
library(pROC)
library(DBI)
library(googleAuthR)
`%notin%` <- Negate(`%in%`)


scopes=c(
  "https://www.googleapis.com/auth/cloud-platform",
  "https://spreadsheets.google.com/feeds",
  "https://www.googleapis.com/auth/drive.file",
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive")

options(googleAuthR.scopes.selected = scopes)
service_token <- gar_auth_service(json_file="/home/data/ssh-key.json")
gar_auth_service("/home/data/ssh-key.json")
bq_auth(path = "/home/data/ssh-key.json")



projectid<-'project_id'
datasetid<-'id'
bq_conn <-  dbConnect(bigquery(), 
                      project = "fiona-s-farm",
                      dataset = "model_variables", 
                      use_legacy_sql = FALSE
)

## write query here. if it is a classiication problem, target column must be factor if quantity then numeric.
tb = bq_project_query("fiona-s-farm", "select 
t1.user_id,
t1.is_d1 as target,
coalesce(t2.max_lvl_d0,0) as max_lvl_d0
from Looker_db.user_summary t1
left join `fiona-s-farm.model_variables.puzzle_variables` t2 on t1.user_id = t2.user_id")
mdl_full <- bq_table_download(tb)

targ_avg <- mean(mdl_full[["target"]])
mdl_full["target_factor"] = as.factor(mdl_full[["target"]])     #target casted as factor
mdl_full[is.na(mdl_full)] <- 0

train_base = mdl_full[,c("max_lvl_d0","target_factor")]
result_base = mdl_full[,c("max_lvl_d0","target")]

controls = rpart.control(minsplit = 110 , minbucket = 35, xval=10, maxdepth = 10, cp = 0.00001) 
fit = rpart(target_factor ~ ., data = train_base,parms = list(split = "gini"), control = controls)

#To see the decision tree graph use the following
#rpart.plot(fit)  

result_base["bucket"] = fit$where

result_base <- result_base %>% group_by(bucket) %>% summarize(target_rate = mean(target), user_count = n(), minn = min(max_lvl_d0), maxx = max(max_lvl_d0)) %>% arrange(minn)
result_base <- result_base %>% mutate(bucket_limits = paste(minn,"_",maxx,sep="")) %>% dplyr::select(bucket_limits,bucket_limits, user_count,target_rate)


result_base


