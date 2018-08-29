library(data.table)
library(dplyr)

df_month_all = fread('F:/Sales_Forecast/data_dir/top200/saleshist_m_all.csv',encoding = 'UTF-8')
df_month_all$V1 = NULL
df_month_all$PRODCODE = paste0('0',df_month_all$PRODCODE)
# df_month_all$STORECODE = sprintf("%06d", df_month_all$STORECODE)
df_month_all = df_month_all %>% left_join(df_month_all[,.(count=.N), by=PRODCODE],by = 'PRODCODE') %>% data.table()
# 0200408591_02

df_month_all[df_month_all$month %in% c(201808),]$SALE_QTY_AVG = 
  df_month_all[df_month_all$month %in% c(201808),]$SALE_QTY_AVG*31/22
  

df_month_all[df_month_all$PRODCODE %in% '0200000923',]


### Data Set
idx = df_month_all$count<7
extract_lesssix = df_month_all[idx,]
extract_greatersix = df_month_all[!idx,]

train_set = extract_greatersix

# k11 = train_set[,.(count=.N), by=unique_code]
# k22 = k11[order(k11$count),]

df_list = split(train_set[,c(1,2,5)],train_set$PRODCODE)


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
#           'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/forecast_df_LenGre7_all.csv',
#           row.names = F)



extract_lesssix_list = split(extract_lesssix[,c(1,2,5)],extract_lesssix$PRODCODE)
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


forecast_all_SIXMONTH = rbind(forecast_df,lesssix_forecast_df)

# mean(df_month_all_final$mape)
# 
# write.csv(forecast_all_SIXMONTH,
#           'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/XGBoost_forecast_all_SIXMONTH.csv',
#           row.names = F)


