

library(forecastxgb)
model <- xgbar(gas)
fc <- forecast(model, h = 1)
plot(fc)


library(fpp)
consumption <- usconsumption[ ,1]
income <- matrix(usconsumption[ ,2], dimnames = list(NULL, "Income"))
consumption_model <- xgbar(y = consumption, xreg = income)

income_future <- matrix(forecast(xgbar(usconsumption[,2]), h = 10)$mean, dimnames = list(NULL, "Income"))

plot(forecast(consumption_model, xreg = income_future))

