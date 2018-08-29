library(mongolite)
host <- "10.230.0.125"#主机名
port <- "27017"#端口号
database <- "python_database"#数据库
URL <- paste0("mongodb://",host,":",port,"/",database)
con <-mongo(collection="GZ_data_XL_KC",#表名
        url = URL)

alldata <- con$find('{}')


# 2018-7-23 ~ 2018-7-29

test1 = alldata[50,]
set.seed(25)
myts <- ts(test1$fill_sale_qty[[1]],     # random data
           start = c(1),
           frequency = 1)

library(forecastxgb)
model <- xgbar(myts)
fc <- forecast(model, h = 7)

plot(fc)


fit <- nnetar(tsclean(alldata[k,]$fill_sale_qty[[1]]))



for(k in seq(nrow(alldata))){
  outliers = boxplot(alldata[k,]$fill_sale_qty[[1]], plot=FALSE)$out
  if(length(outliers)==0){
    print(k)
    break
    
  }
}
# outliers = boxplot(test1$fill_sale_qty[[1]], plot=FALSE)$out

boxplot(test1$fill_sale_qty[[1]])


##First create some data 
##You should include this in your question)
set.seed(2)
dd = data.frame(x = rlnorm(26), y=LETTERS)


k22 = 200
g11 = alldata[k22,]$fill_sale_qty[[1]]
alldata[k22,]$data_level
# tsclean(g11)
# plot(g11,tsclean(g11))
plot(g11,type="l",col="red")
lines(tsclean(g11),col="green")

