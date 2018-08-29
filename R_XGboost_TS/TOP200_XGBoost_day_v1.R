library(data.table)
library(dplyr)

df_month_all = fread('F:/Sales_Forecast/data_dir/top200/saleshist_d_ware.csv',encoding = 'UTF-8')
df_month_all$V1 = NULL
df_month_all$PRODCODE = paste0('0',df_month_all$PRODCODE)
# df_month_all$STORECODE = sprintf("%06d", df_month_all$STORECODE)
df_month_all$ALCSCM[df_month_all$ALCSCM=="-"] = '01'
df_month_all$unique_code = paste(df_month_all$PRODCODE,df_month_all$ALCSCM,sep = '_')
df_month_all = df_month_all %>% left_join(df_month_all[,.(count=.N), by=unique_code],by = 'unique_code') %>% data.table()
# 0200408591_02

### Data Set
ForecastDayList = as.character(seq(as.Date("2018-07-01"), as.Date("2018-08-22"), by = "day"))
ForecastDayList = as.integer(gsub('-','',ForecastDayList))

idx = df_month_all$count<(length(ForecastDayList)+7)
extract_lesssix = df_month_all[idx,]
extract_greatersix = df_month_all[!idx,]


train_set = extract_greatersix[!extract_greatersix$DT %in% ForecastDayList,]
test_set = extract_greatersix[extract_greatersix$DT %in% ForecastDayList,]

# k11 = train_set[,.(count=.N), by=unique_code]
# k22 = k11[order(k11$count),]

df_list_train = split(train_set[,c(4,7,8)],train_set$unique_code)
df_list_test = split(test_set[,c(4,7,8)],test_set$unique_code)


func_model <- function(row_j){
  set.seed(row_j)
  row_data = df_list_train[row_j][[1]]$SALE_QTY_AVG
  #tsclean
  # library(forecast)
  # row_data = tsclean(row_data)
  myts <- ts(row_data,     # random data
             start = c(1),
             frequency = 1)
  
  library(forecastxgb)
  model <- xgbar(myts)
  fc <- forecast(model, h = nrow(df_list_test[row_j][[1]]))
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


forecast_month = rep(c(201807,201808),length(df_list))
forecast_names = c()
forecast_value = c()
for(k in seq(length(df_list))){
  forecast_value = append(forecast_value,forecast_data[k][[1]])
  forecast_names = append(forecast_names,c(names(df_list[k]),names(df_list[k])))
}

forecast_df = data.frame(names=forecast_names,month=forecast_month,forecast_value = forecast_value)
# write.csv(forecast_df,
#           'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m_ware/forecast_df_LenGre7.csv',
#           row.names = F)

