library(mongolite)
host <- "10.230.0.125"#主机名
port <- "27017"#端口号
database <- "python_database"#数据库
URL <- paste0("mongodb://",host,":",port,"/",database)
con <-mongo(collection="GZ_data_XL_KC",#表名
        url = URL)

alldata <- con$find('{}')

# test set:true_7day_df
true_7day = as.character(seq(as.Date("2018-07-23"), as.Date("2018-07-29"), by = "day"))
true_7day_df = c()
for(j in seq(nrow(alldata))){
  row1_date = alldata[j,]$date[[1]]
  row1_qty = alldata[j,]$sale_qty[[1]]
  row1_index = row1_date>= "2018-07-23" & row1_date <= "2018-07-29"
  row1_existdate = row1_date[row1_index]
  row1_existqty = row1_qty[row1_index]
  vl = match(true_7day,row1_existdate)
  vl[!is.na(vl)] = row1_existqty
  true_7day_df = rbind(true_7day_df,vl)
}

true_7day_df = as.data.frame(true_7day_df,row.names = alldata$names)
colnames(true_7day_df) = true_7day
true_7day_df$data_level = alldata['data_level']


data_weather = read.csv('F:/xgm_201806/POC/mongo/collections/GUANGZHOU_weather.csv',header = FALSE,col.names=c('date','tianqi','wendu'))
data_weather$date = as.character(as.Date(data_weather$date))
data_weather$tianqi[is.na(data_weather$tianqi)] = 0


# train_set
train_set = alldata
tianqi_list = list()
wendu_list = list()
for(j in seq(nrow(train_set))){
  raw_date = train_set[j,]$date[[1]]
  if(any(duplicated(raw_date))){
    raw_date_index = which(!duplicated(raw_date))
    row1_date = raw_date[raw_date_index]
  } else{
    row1_date= rain_set[j,]$date[[1]]
  }

  
  row1_qty = train_set[j,]$fill_sale_qty[[1]][raw_date_index]
  row1_index = row1_date < "2018-07-23" & row1_date >= "2017-01-01"
  row1_existdate = row1_date[row1_index]
  row1_existqty = row1_qty[row1_index]
  train_set[j,]$date[[1]]= row1_existdate
  train_set[j,]$fill_sale_qty[[1]]= row1_existqty
  
  row1_stock_qty = train_set[j,]$stock_qty[[1]]
  train_set[j,]$stock_qty[[1]] = row1_stock_qty[row1_index]
  
  #weather
  row1_weather = data_weather[data_weather$date %in% row1_existdate,]
  
  tianqi_list[[j]] = row1_weather$tianqi
  wendu_list[[j]] = row1_weather$wendu
  
}

train_set$tianqi = tianqi_list
train_set$wendu = wendu_list




train_set_1 = subset(train_set,data_level==1)
train_set_2 = subset(train_set,data_level==2)
train_set_3 = subset(train_set,data_level==3)

# 
# predict_7day_df = c()
# for(j in seq(nrow(train_set_3))){
#   set.seed(j)
#   row_data = train_set_3[j,]$fill_sale_qty[[1]]
#   consumption = ts(row_data,start = c(1),frequency = 1)
#   row_stock_qty = train_set_3[j,]$stock_qty[[1]]
#   row_stock_qty[is.na(row_stock_qty)] = 0
#   row_stock_qty_ts = ts(row_stock_qty,start = c(1),frequency = 1)
#   income <- matrix(row_stock_qty, dimnames = list(NULL, "Income"))
#   
#   consumption_model <- xgbar(y = consumption, xreg = income)
#   
#   income_future <- matrix(forecast(xgbar(row_stock_qty_ts), h = 7)$mean, dimnames = list(NULL, "Income"))
#   
#   fore_day = forecast(consumption_model, xreg = income_future)
#   row_predict = as.data.frame(fore_day)
#   predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
#   
# }
# 


func_model <- function(row_j){
  library(forecastxgb)
  set.seed(row_j)
  row_data = train_set_3[row_j,]$fill_sale_qty[[1]]
  consumption = ts(row_data,start = c(1),frequency = 1)
  
  row_tianqi = train_set_3[row_j,]$tianqi[[1]]
  row_wendu = train_set_3[row_j,]$wendu[[1]]
  row_tianqi_ts = ts(row_tianqi,start = c(1),frequency = 1)
  row_wendu_ts = ts(row_wendu,start = c(1),frequency = 1)
  
  
  # df_tqwd = data.frame(row_tianqi=row_tianqi,row_wendu=row_wendu)

  income1 <- matrix(row_tianqi, dimnames = list(NULL, "tianqi"))
  income2 <- matrix(row_wendu, dimnames = list(NULL, "wendu"))
  
  consumption_model <- xgbar(y = consumption, xreg =  cbind(income1,income2))
  
  # income_future1 <- matrix(forecast(xgbar(row_tianqi_ts), h = 7)$mean, dimnames = list(NULL, "tianqi"))
  # income_future2 <- matrix(forecast(xgbar(row_wendu_ts), h = 7)$mean, dimnames = list(NULL, "wendu"))
  income_future1 = matrix(data_weather[data_weather$date %in% as.character(seq(as.Date("2018-07-23"), as.Date("2018-07-29"), by = "day")),]$tianqi,
                          dimnames = list(NULL, "tianqi"))
  income_future2 = matrix(data_weather[data_weather$date %in% as.character(seq(as.Date("2018-07-23"), as.Date("2018-07-29"), by = "day")),]$wendu,
                          dimnames = list(NULL, "wendu"))
  
  fore_day = forecast(consumption_model, xreg = cbind(income_future1,income_future2))
  row_predict = as.data.frame(fore_day)
  # predict_7day_df = rbind(predict_7day_df,row_predict$`Point Forecast`)
  return(row_predict$`Point Forecast`)
}



library(parallel)
library(doParallel)
cl.cores <- detectCores()
cl <- makeCluster(cl.cores)
registerDoParallel(cl)
forecast_7 = foreach(x=seq(nrow(train_set_3)),
                         .export=ls(envir=globalenv())) %dopar% func_model(x)

stopCluster(cl)


fore_7_df = c()
for(k in seq(nrow(train_set_3))){
  fore_7_df = rbind(fore_7_df,forecast_7[[k]])
}

predict_7day_df = as.data.frame(fore_7_df,row.names = train_set_3$names)
colnames(predict_7day_df) = paste('forecast',true_7day,sep = '-')
predict_7day_df$forecast_total = apply(predict_7day_df,1,sum,na.rm=TRUE)

true_7day_df_3 = subset(true_7day_df,data_level==3)
true_7day_df_3$true_total = apply(true_7day_df_3[,1:7],1,function(row) mean(row,na.rm = TRUE)*length(row))
all_3 = cbind(true_7day_df_3,predict_7day_df)

all_output = all_3[,c('data_level','true_total','forecast_total')]
all_output$mape = abs(all_output$true_total-all_output$forecast_total)/all_output$true_total

mean(all_output$mape[!is.nan(all_output$mape)])






