library(dplyr)
library(data.table)
library(xlsx)
options(warn=-1)


### parameter
set_forecast_month_len = 3
require(lubridate)
last_month = Sys.Date()
month(last_month) <- month(last_month) + set_forecast_month_len
begin_month = ceiling_date(last_month, "month") - months(set_forecast_month_len)

set_forecast_month_orin = seq(begin_month, last_month, by = "month")
set_forecast_month = as.numeric(format(set_forecast_month_orin,"%Y%m"))

### Data Set
sale_df = fread('./input_output_file/sale_data_top100_month.csv',encoding = 'UTF-8')
sale_df$date = as.numeric(gsub("-","",sale_df$date))
sale_df_set = sale_df[,c('good_gid','date','uniq_ID','sale_qty_perstore_perdays',
                         'HB_temp','TB_temp','holiday_data','count','days_count','ALCSCM','warename')]
colnames(sale_df_set)[8] = "store_count"
df_month_all = sale_df_set %>% left_join(sale_df_set[,.(count=.N), by=uniq_ID],by = 'uniq_ID') %>% data.table()
df_month_all$ALCSCM = paste0('0',df_month_all$ALCSCM)
colnames(df_month_all)[12] = 'TS_count'

holiday_month = read.xlsx("./input_output_file/holiday_update201809.xlsx", sheetName="month")
holiday_month = holiday_month[,c('month','holiday_count_month')]
colnames(holiday_month)[2] = 'holiday_data'
holiday_month$month = as.numeric(gsub('-','',substr(holiday_month$month,1,7)))
forcast_month = holiday_month[holiday_month$month %in% set_forecast_month,]$holiday_data



sale_df_day = fread('./input_output_file/sale_data_top100_day.csv',encoding = 'UTF-8')
max_day = as.Date(max(sale_df_day$date))
smooth_day = as.numeric(days_in_month(max_day))/as.numeric(format(max_day,"%d"))
  
# train_set = df_month_all[!df_month_all$date %in% c(201807,201808),]
# test_set = df_month_all[df_month_all$date %in% c(201807,201808),]
df_month_all[df_month_all$date %in% c(as.numeric(format(Sys.Date(),"%Y%m"))),c('sale_qty_perstore_perdays')] = 
  df_month_all[df_month_all$date %in% c(as.numeric(format(Sys.Date(),"%Y%m"))),c('sale_qty_perstore_perdays')]*smooth_day

df_list = split(df_month_all[,c(3,2,4,5,6,7)],df_month_all$uniq_ID)

# Exponential smoothing 
SES_model <- function(forecast_h){
  library(forecast)
  forcast_values = c()
  for(row_j in seq_len(length(df_list))){
    
    
    row_data = df_list[row_j][[1]]$sale_qty_perstore_perdays
    row_data = df_list[row_j][[1]]$sale_qty_perstore_perdays
    consumption = ts(row_data,start = c(1),frequency = 1)
    if(length(row_data)>=2){
      model <- as.data.frame(ses(consumption, initial = "optimal", h=forecast_h, beta=NULL, gamma=NULL))
      row_predict = model$`Point Forecast` # h is the no. periods to forecas
    }else{
      row_predict = rep(mean(row_data),forecast_h)
      
    }
    forcast_values = append(forcast_values,row_predict)
    
    cat('row:',row_j,'\n')
    
    
  }
  
  
  forecast_month = rep(set_forecast_month,length(df_list))
  forecast_names = rep(names(df_list),each=forecast_h)
  forecast_df = data.frame(uniq_ID=forecast_names,
                           date=forecast_month,
                           forcast_values = forcast_values,
                           stringsAsFactors=FALSE)
  
  return(forecast_df)
}

# ARIMA
ARIMA_model <- function(forecast_h){
  library(forecast)
  require(smooth)
  require(Mcomp)
  forcast_values = c()
  for(row_j in seq_len(length(df_list))){
    
    row_data = df_list[row_j][[1]]$sale_qty_perstore_perdays
    row_data = df_list[row_j][[1]]$sale_qty_perstore_perdays
    consumption = ts(row_data,start = c(1),frequency = 1)
    
    if(length(row_data)>7){

      set.seed(row_j)
      skirtsarima<-auto.arima(consumption,trace=F)
      skirtsarimaforecast<-forecast(skirtsarima,h=forecast_h)
      row_predict = as.data.frame(skirtsarimaforecast)$`Point Forecast`
      
    } else{
      
      if(length(consumption)>=3){
        
        set.seed(row_j)
        model = sma(consumption,order=3, h=forecast_h, silent=FALSE)
        row_predict = as.numeric(model$forecast)
      } else{
        # 
        # set.seed(row_j)
        # model = sma(consumption,order=length(consumption), h=2, silent=FALSE)
        # row_predict = as.numeric(model$forecast)
        row_predict = rep(mean(row_data),forecast_h)
        
      }
      
      
    }
    # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
    forcast_values = append(forcast_values,row_predict)
    cat('row:',row_j,'\n')
    
  }
  
  forecast_month = rep(set_forecast_month,length(df_list))
  forecast_names = rep(names(df_list),each=forecast_h)
  forecast_df = data.frame(uniq_ID=forecast_names,
                           date=forecast_month,
                           forcast_values = forcast_values,
                           stringsAsFactors=FALSE)
  
  return(forecast_df)
  
}

# XGBoost
XGBoost_model <- function(forecast_h){
  require(smooth)
  require(Mcomp)
  library(forecast)
  library(forecastxgb)
  # source("F:/Sales_Forecast/data_dir/top200/XGBoost/utils.R")
  # source("F:/Sales_Forecast/data_dir/top200/XGBoost/xgbar.R")
  source("/home/test/nby/R_wsp/utils.R")
  source("/home/test/nby/R_wsp/xgbar.R")
  
  forcast_values = c()
  for(row_j in seq_len(length(df_list))){
    
    row_data = df_list[row_j][[1]]$sale_qty_perstore_perdays
    row_data = df_list[row_j][[1]]$sale_qty_perstore_perdays
    consumption = ts(row_data,start = c(1),frequency = 1)
    
    if(length(row_data)>=12){
      row_HB_temp = df_list[row_j][[1]]$HB_temp
      row_TB_temp = df_list[row_j][[1]]$TB_temp
      row_holiday = df_list[row_j][[1]]$holiday_data
      
      row_HB_temp_ts = ts(row_HB_temp,start = c(1),frequency = 1)
      row_TB_temp_ts = ts(row_TB_temp,start = c(1),frequency = 1)
      row_holiday_ts = ts(row_holiday,start = c(1),frequency = 1)
      
      
      # df_tqwd = data.frame(row_tianqi=row_tianqi,row_wendu=row_wendu)
      
      income1 <- matrix(row_HB_temp, dimnames = list(NULL, "HB_temp"))
      income2 <- matrix(row_TB_temp, dimnames = list(NULL, "TB_temp"))
      income3 <- matrix(row_holiday, dimnames = list(NULL, "holiday_data"))
      
      set.seed(row_j)
      consumption_model <- xgbar(y = consumption, xreg =  cbind(income1,income2,income3))
      
      
      income_future1 <- matrix(forecast(xgbar(row_HB_temp_ts), h = forecast_h)$mean, dimnames = list(NULL, "HB_temp"))
      income_future2 <- matrix(forecast(xgbar(row_TB_temp_ts), h = forecast_h)$mean, dimnames = list(NULL, "TB_temp"))
      income_future3 = matrix(forcast_month[1:forecast_h],dimnames = list(NULL, "holiday_data"))
      
      fore_day = forecast(consumption_model, xreg = cbind(income_future1,income_future2,income_future3))
      row_predict = as.data.frame(fore_day)$`Point Forecast`
      
    } else if(length(row_data)>7){
      # 
      set.seed(row_j)
      skirtsarima<-auto.arima(consumption,trace=T)
      skirtsarimaforecast<-forecast(skirtsarima,h=forecast_h)
      row_predict = as.data.frame(skirtsarimaforecast)$`Point Forecast`
      
    } else{
      
      if(length(consumption)>=3){
        
        set.seed(row_j)
        model = sma(consumption,order=3, h=forecast_h, silent=FALSE)
        row_predict = as.numeric(model$forecast)
      } else{
        # 
        # set.seed(row_j)
        # model = sma(consumption,order=length(consumption), h=2, silent=FALSE)
        # row_predict = as.numeric(model$forecast)
        row_predict = rep(mean(row_data),forecast_h)
        
      }
      
      
    }
    # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
    forcast_values = append(forcast_values,row_predict)
    cat('row:',row_j,'\n')
    
    
  }

  
  forecast_month = rep(set_forecast_month,length(df_list))
  forecast_names = rep(names(df_list),each=forecast_h)
  forecast_df = data.frame(uniq_ID=forecast_names,
                           date=forecast_month,
                           forcast_values = forcast_values,
                           stringsAsFactors=FALSE)
  
  return(forecast_df)
}


# Map
SES_forecast = SES_model(set_forecast_month_len)
colnames(SES_forecast)[3] = 'SES_forecast_values'
ARIMA_forecast = ARIMA_model(set_forecast_month_len)
colnames(ARIMA_forecast)[3] = 'ARIMA_forecast_values'
XGB_forecast = XGBoost_model(set_forecast_month_len)
colnames(XGB_forecast)[3] = 'XGB_forecast_values'

add_col = df_month_all[df_month_all$date %in% c(201809),c('good_gid','uniq_ID','ALCSCM','warename','TS_count','store_count')]

df_merge = SES_forecast %>% 
  left_join(ARIMA_forecast,by = c('uniq_ID','date')) %>% 
  left_join(XGB_forecast,by = c('uniq_ID','date')) %>%
  left_join(add_col,by = c('uniq_ID')) %>% data.table()

df_merge$forecast_values = 0.5*df_merge$SES_forecast_values+0.3*df_merge$ARIMA_forecast_values+0.2*df_merge$XGB_forecast_values

add_day_col = data.frame(date=set_forecast_month,
                         days_in_month=as.numeric(days_in_month(set_forecast_month_orin)))
df_merge = df_merge %>% left_join(add_day_col,by = c('date')) %>% data.table()
df_merge$forecast_values_month = df_merge$forecast_values*df_merge$days_in_month
df_output = df_merge[,c(6,2,7:13)]
OUTPUT_FILE = paste0("./input_output_file/TOP100_modelmerge_forecast_month_",format(Sys.time(), "%Y%m%d%H%M%S"),".xlsx")
write.xlsx(df_output, OUTPUT_FILE,row.names=F, sheetName='modelmerge')



