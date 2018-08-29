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
# 0200046641_04

df_month_all[df_month_all$month %in% c(201808),]$SALE_QTY_AVG = 
  df_month_all[df_month_all$month %in% c(201808),]$SALE_QTY_AVG*31/22

# df_month_all[df_month_all$unique_code %in% '0200000923_01',]


### Data Set

idx = df_month_all$count<7
extract_lesssix = df_month_all[idx,]
extract_greatersix = df_month_all[!idx,]

# k11 = train_set[,.(count=.N), by=unique_code]
# k22 = k11[order(k11$count),]

df_list = split(extract_greatersix[,c(4,7,8)],extract_greatersix$unique_code)


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
  fc <- forecast(model, h = 6)
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


forecast_month = rep(c(201809,201810,201811,201812,201901,201902),length(df_list))
forecast_names = c()
forecast_value = c()
for(k in seq(length(df_list))){
  forecast_value = append(forecast_value,forecast_data[k][[1]])
  forecast_names = append(forecast_names,rep(names(df_list[k]),6))
}

forecast_df = data.frame(PRODCODE=forecast_names,month=forecast_month,forecast_value = forecast_value)
# write.csv(forecast_df,
#           'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/forecast_df_LenGre7_ware.csv',
#           row.names = F)


extract_lesssix_list = split(extract_lesssix[,c(4,7,8)],extract_lesssix$unique_code)
lesssix_forecast_month = rep(c(201809,201810,201811,201812,201901,201902),length(extract_lesssix_list))
lesssix_forecast_names = c()
lesssix_forecast_value = c()
for(j in seq(length(extract_lesssix_list))){
  row_data = extract_lesssix_list[j][[1]]$SALE_QTY_AVG
  
  lesssix_forecast_value = append(lesssix_forecast_value,rep(mean(row_data),6))
  lesssix_forecast_names = append(lesssix_forecast_names,rep(names(extract_lesssix_list[j]),6))
  
}

lesssix_forecast_df = data.frame(PRODCODE=lesssix_forecast_names,
                                 month=lesssix_forecast_month,
                                 forecast_value = lesssix_forecast_value)


basic_info = df_month_all[,c(1,2,3,8)]
basic_info= basic_info[!duplicated(basic_info),]
forecast_df = read.csv('F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/forecast_df_LenGre7_ware.csv')
forecast_ware_SIXMONTH = rbind(forecast_df,lesssix_forecast_df)
colnames(forecast_ware_SIXMONTH)[1] = 'unique_code'
forecast_ware_SIXMONTH$unique_code = as.character(forecast_ware_SIXMONTH$unique_code)
forecast_ware_SIXMONTH = forecast_ware_SIXMONTH %>% left_join(basic_info,by = 'unique_code') %>% data.table()

forecast_ware_SIXMONTH = forecast_ware_SIXMONTH[,c(4,5,6,2,3)]
# library(xlsx)
# write.xlsx(forecast_ware_SIXMONTH,'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/XGBoost_forecast_ware_SIXMONTH.xlsx')

# df_month_all[df_month_all$unique_code %in%'0200406471_07',]



