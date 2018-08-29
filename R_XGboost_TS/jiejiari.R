
Generate_holiday_data <- function(date_list){
  if(length(date_list)==0){stop("input error")}
  chunjie_2016 = seq(as.Date("2016-02-07"),as.Date("2016-02-13"), by = "day")## chunjie
  qingming_2016=seq(as.Date("2016-04-02"),as.Date("2016-04-04"), by = "day")  ## qingming
  wuyi_2016=seq(as.Date("2016-04-29"),as.Date("2016-05-01"), by = "day")## 5-1
  duanwu_2016=seq(as.Date("2016-06-09"),as.Date("2016-06-11"), by = "day")## duanwu
  liuyi_2016 = seq(as.Date("2016-05-31"),as.Date("2016-06-02"), by = "day")## 6-1
  shiyi_2016=seq(as.Date("2016-10-01"),as.Date("2016-10-07"), by = "day")## 10-1
  liuyiba_2016 = as.Date("2016-06-18") ##6-18
  shuangshiyi_2016 = as.Date("2016-11-11") ##11-11
  shengdan_2016 = as.Date(c("2016-12-24","2016-12-25")) ##shengdan
  yuandan_2016 = seq(as.Date("2016-01-01"),as.Date("2016-01-03"), by = "day") ##shengdan
  
  
  chunjie_2017 = seq(as.Date("2017-01-27"),as.Date("2017-02-02"), by = "day")## chunjie
  qingming_2017=seq(as.Date("2017-04-02"),as.Date("2017-04-04"), by = "day")  ## qingming
  wuyi_2017=seq(as.Date("2017-04-29"),as.Date("2017-05-01"), by = "day")## 5-1
  duanwu_2017=seq(as.Date("2017-05-28"),as.Date("2017-05-30"), by = "day")## duanwu
  liuyi_2017 = seq(as.Date("2017-05-31"),as.Date("2017-06-02"), by = "day")## 6-1
  shiyi_2017=seq(as.Date("2017-10-01"),as.Date("2017-10-07"), by = "day")## 10-1
  liuyiba_2017 = as.Date("2017-06-18") ##6-18
  shuangshiyi_2017 = as.Date("2017-11-11") ##11-11
  shengdan_2017 = as.Date(c("2017-12-24","2017-12-25")) ##shengdan
  yuandan_2017 = seq(as.Date("2016-12-31"),as.Date("2017-01-02"), by = "day") ##shengdan
  
  
  chunjie_2018=seq(as.Date("2018-02-15"),as.Date("2018-02-21"), by = "day")## chunjie
  qingming_2018=seq(as.Date("2018-04-05"),as.Date("2018-04-07"), by = "day")## qingming
  wuyi_2018=seq(as.Date("2018-04-29"),as.Date("2018-05-01"), by = "day")##	5-1
  duanwu_2018=seq(as.Date("2018-06-16"),as.Date("2018-06-18"), by = "day")## duanwu
  liuyi_2018 = seq(as.Date("2018-05-31"),as.Date("2018-06-02"), by = "day")## 6-1
  shiyi_2018=seq(as.Date("2018-10-01"),as.Date("2018-10-07"), by = "day")## 10-1
  liuyiba_2018 = as.Date("2018-06-18") ##6-18
  shuangshiyi_2018 = as.Date("2018-11-11") ##11-11
  shengdan_2018 = as.Date(c("2018-12-24","2018-12-25")) ##shengdan
  yuandan_2018 = seq(as.Date("2017-12-30"),as.Date("2018-01-01"), by = "day") ##shengdan
  
  
  
  chunjie = c(chunjie_2016,chunjie_2017,chunjie_2018)
  qingming = c(qingming_2016,qingming_2017,qingming_2018)
  wuyi = c(wuyi_2016,wuyi_2017,wuyi_2018)
  duanwu = c(duanwu_2016,duanwu_2017,duanwu_2018)
  liuyi = c(liuyi_2016,liuyi_2017,liuyi_2018)
  shiyi = c(shiyi_2016,shiyi_2017,shiyi_2018)
  liuyiba = c(liuyiba_2016,liuyiba_2017,liuyiba_2018)
  shuangshiyi = c(shuangshiyi_2016,shuangshiyi_2017,shuangshiyi_2018)
  shengdan = c(shengdan_2016,shengdan_2017,shengdan_2018)
  yuandan = c(yuandan_2016,yuandan_2017,yuandan_2018)
  
  
  output_data = rep(0,length(date_list))
  
  output_data[date_list %in% chunjie] = 1 ## chunjie
  output_data[date_list %in% qingming] = 2## qingming
  output_data[date_list %in% wuyi] = 3##	5-1
  output_data[date_list %in% duanwu] = 4## duanwu
  output_data[date_list %in% liuyi] = 5## 6-1
  output_data[date_list %in% shiyi] = 6## 10-1
  output_data[date_list %in% liuyiba] = 7##6-18
  output_data[date_list %in% shuangshiyi] = 8##11-11
  output_data[date_list %in% shengdan] = 9##shengdan
  output_data[date_list %in% yuandan] = 10##yuandan
  
  
  return(output_data)
}



# date_interest1="2017-01-01"
# date_interest2="2018-07-18"
# date.seq1=seq(as.Date(date_interest1),as.Date(date_interest2), by = "day")
# 



date_list = seq(as.Date("2016-01-01"), as.Date("2018-12-31"), by = "day")
output_data = Generate_holiday_data(date_list)

date_2 = rep(NA,length(date_list))
date_2[date_list %in% chunjie] = '春节' ## chunjie
date_2[date_list %in% qingming] = '清明' ## qingming
date_2[date_list %in% wuyi] = '五一劳动节' ##	5-1
date_2[date_list %in% duanwu] = '端午' ## duanwu
date_2[date_list %in% liuyi] = '六一儿童节' ## 6-1
date_2[date_list %in% shiyi] ='十一国庆节'  ## 10-1
date_2[date_list %in% liuyiba] = '618'  ##6-18
date_2[date_list %in% shuangshiyi] = '双十一'  ##11-11
date_2[date_list %in% shengdan] = '圣诞节' ##shengdan
date_2[date_list %in% yuandan] = '元旦'  ##yuandan

jiejiari = data.frame(date=date_list,encode=output_data,type=date_2)

# write.csv(jiejiari,'F:/Sales_Forecast/data_dir/holiday_update20180821.csv')

