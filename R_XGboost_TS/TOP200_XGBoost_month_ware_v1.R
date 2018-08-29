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

### Data Set
idx = df_month_all$count<7
extract_lesssix = df_month_all[idx,]
extract_greatersix = df_month_all[!idx,]

train_set = extract_greatersix[!extract_greatersix$month %in% c(201807,201808),]
test_set = extract_greatersix[extract_greatersix$month %in% c(201807,201808),]

# k11 = train_set[,.(count=.N), by=unique_code]
# k22 = k11[order(k11$count),]

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


forecast_month = rep(c(201807,201808),length(df_list))
forecast_names = c()
forecast_value = c()
for(k in seq(length(df_list))){
  forecast_value = append(forecast_value,forecast_data[k][[1]])
  forecast_names = append(forecast_names,c(names(df_list[k]),names(df_list[k])))
}

forecast_df = data.frame(names=forecast_names,month=forecast_month,forecast_value = forecast_value)
# write.csv(forecast_df,
#           'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/forecast_df_LenGre7_ware.csv',
#           row.names = F)

extract_lesssix_list = split(extract_lesssix[,c(4,7,8)],extract_lesssix$unique_code)
lesssix_forecast_month = c()
lesssix_forecast_names = c()
lesssix_forecast_value = c()
for(j in seq(length(extract_lesssix_list))){
  row_data = extract_lesssix_list[j][[1]]$SALE_QTY_AVG
  if(length(row_data)==1){
    lesssix_forecast_month = append(lesssix_forecast_month,c(201808))
    lesssix_forecast_value = append(lesssix_forecast_value,mean(row_data))
    lesssix_forecast_names = append(lesssix_forecast_names,
                                    names(extract_lesssix_list[j]))
    
  }
  else{
    lesssix_forecast_month = append(lesssix_forecast_month,c(201807,201808))
    lesssix_forecast_value = append(lesssix_forecast_value,c(mean(row_data),mean(row_data)))
    lesssix_forecast_names = append(lesssix_forecast_names,
                                    c(names(extract_lesssix_list[j]),
                                      names(extract_lesssix_list[j])))
  }
  
  
}


lesssix_forecast_df = data.frame(names=lesssix_forecast_names,
                                 month=lesssix_forecast_month,
                                 forecast_value = lesssix_forecast_value)


forecast_df = read.csv('F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/forecast_df_LenGre7_ware.csv')
# forecast_df$names = paste0('0',forecast_df$names)

forecast_all = rbind(forecast_df,lesssix_forecast_df)
forecast_all[forecast_all$month %in% c(201808),]$forecast_value = forecast_all[forecast_all$month %in% c(201808),]$forecast_value*22/31
colnames(forecast_all)[1] = 'unique_code'
forecast_all$unique_code =as.character(forecast_all$unique_code)
# 0200046641_04_201808
dele_index = which(forecast_all$unique_code=='0200046641_04' & forecast_all$month==201808)
forecast_all = forecast_all[-c(dele_index),]

df_month_all_78 = df_month_all[df_month_all$month %in% c(201807,201808),]
df_month_all_final = df_month_all_78 %>% left_join(forecast_all,by = c('unique_code','month')) %>% data.table()

df_month_all_final[df_month_all_final$unique_code %in% '0200408591_02',]$forecast_value = df_month_all_final[df_month_all_final$unique_code %in% '0200408591_02',]$SALE_QTY_AVG
df_month_all_final$mape = abs(df_month_all_final$SALE_QTY_AVG-df_month_all_final$forecast_value)/df_month_all_final$SALE_QTY_AVG

# write.csv(df_month_all_final,
#           'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/XGBoost_month_ware_mape.csv',
#           row.names = F)

