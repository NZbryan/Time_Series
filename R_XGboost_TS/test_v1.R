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

base_path = 'F:/Sales_Forecast/data_dir/top200/XGBoost/forecast_m/all_model'

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
  # save model
  model_path = paste0(base_path,'/',names(df_list[row_j]),'.rds')
  saveRDS(model, file=model_path)
  
  fc <- forecast(model, h = 6)
  row_predict = as.data.frame(fc)
  # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
  return(row_predict$`Point Forecast`)
}






