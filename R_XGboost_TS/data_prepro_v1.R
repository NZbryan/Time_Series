library(data.table)
library(dplyr)

df_month_all = fread('F:/Sales_Forecast/data_dir/top200/saleshist_m_ware.csv',encoding = 'UTF-8')
df_month_all$V1 = NULL
df_month_all$PRODCODE = paste0('0',df_month_all$PRODCODE)
# df_month_all$STORECODE = sprintf("%06d", df_month_all$STORECODE)
df_month_all$ALCSCM[df_month_all$ALCSCM=="-"] = '01'
df_month_all$unique_code = paste(df_month_all$PRODCODE,df_month_all$ALCSCM,sep = '_')
df_month_all = df_month_all %>% left_join(df_month_all[,.(count=.N), by=unique_code],by = 'unique_code') %>% data.table()
# 0200408591_02

### Data Set
idx = df_month_all$count<6
extract_lesssix = df_month_all[idx,]
extract_greatersix = df_month_all[!idx,]

train_set = extract_greatersix[!extract_greatersix$month %in% c(201807,201808),]
test_set = extract_greatersix[extract_greatersix$month %in% c(201807,201808),]


df_list = split(train_set[,c(4,7,8)],train_set$unique_code)


func_model <- function(row_j){
  set.seed(row_j)
  row_data = df_list[row_j][[1]]$SALE_QTY_AVG
  #tsclean
  # library(forecast)
  # row_data = tsclean(row_data)
  myts <- ts(row_data,     # random data
             start = c(1),
             frequency = 1)
  
  library(forecastxgb)
  model <- xgbar(myts)
  fc <- forecast(model, h = 2)
  row_predict = as.data.frame(fc)
  # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
  return(row_predict$`Point Forecast`)
}


library(parallel)
library(doParallel)
cl.cores <- detectCores()
cl <- makeCluster(cl.cores-2)
registerDoParallel(cl)
forecast_data = foreach(x=seq(length(df_list)),
                     .export=ls(envir=globalenv())) %dopar% func_model(x)
stopCluster(cl)



for(row_j in seq(length(df_list))){
  set.seed(row_j)
  row_data = df_list[row_j][[1]]$SALE_QTY_AVG
  #tsclean
  # library(forecast)
  # row_data = tsclean(row_data)
  myts <- ts(row_data,     # random data
             start = c(1),
             frequency = 1)
  
  library(forecastxgb)
  model <- xgbar(myts)
  fc <- forecast(model, h = 2)
  row_predict = as.data.frame(fc)
  # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
  cat("finsh:",names(df_list[row_j]),row_j)
}


df_month_all[df_month_all$unique_code %in% c('0100036791_04')]
