options(java.parameters = "-Xmx8192m")
library('data.table')
library('dplyr')
library('reshape2')
library('xlsx')
library('RJDBC')

gc()
print("Data update starts...")
print(Sys.time())

#database connect
drv <- JDBC("oracle.jdbc.OracleDriver", classPath="/xgm/jdbc_driver/ojdbc6.jar")
dbconn <- dbConnect(drv, "jdbc:oracle:thin:@//192.168.80.160:1521/MCPOSDB", "it_zheng", "it_zheng")

prod_info = dbGetQuery(dbconn, 'SELECT gid, code, sort, sname4, name, code2 FROM goods g LEFT JOIN v_sortall v ON g.sort = v.scode4') 
prod_info = data.table(prod_info)
prod_info = prod_info[,.(prodgid = GID, prodcode = CODE, prodname = NAME, catecode = SORT, catename = SNAME4, oldcode = CODE2)]
fwrite(prod_info, file = "/xgm/R_scripts/monthly_realloc/input/prod_info.csv")
fwrite(prod_info, file = "/xgm/R_scripts/usrdef_realloc/input/prod_info.csv")

store_info = dbGetQuery(dbconn, 'SELECT gid, code, name FROM store')
store_info = data.table(store_info)
store_info = store_info[,.(storegid = GID, storecode = CODE, storename = NAME)]
fwrite(store_info, file = "/xgm/R_scripts/monthly_realloc/input/store_info.csv")
fwrite(store_info, file = "/xgm/R_scripts/usrdef_realloc/input/store_info.csv")

businv = dbGetQuery(dbconn, 'SELECT gdgid, store, qty FROM businvs WHERE qty > 0')
businv = data.table(businv)
businv = businv[,.(prodgid = GDGID, storegid = STORE, qty = QTY)]

businv = businv %>% 
  left_join(store_info, by ='storegid') %>% 
  left_join(prod_info, by = 'prodgid') %>% 
  data.table()
businv = businv[,.(storecode, catecode, prodcode, qty)]
fwrite(businv, file = "/xgm/R_scripts/monthly_realloc/input/businv.csv")
fwrite(businv, file = "/xgm/R_scripts/usrdef_realloc/input/businv.csv")

SQL_cmd = paste("SELECT SUBSTR(b2.posno,0,6) storecode, g.code prodcode, SUBSTR(b2.flowno,0,8) dt, SUM(qty) day_qty FROM buy2 b2 LEFT JOIN goods g ON b2.gid = g.gid WHERE SUBSTR(g.sort,0,2) NOT IN ('80', '90', '91') ",
                "AND SUBSTR(b2.flowno,0,8) >= ", "'", gsub("-","", Sys.Date() - 30), "'", " AND SUBSTR(b2.flowno,0,8) <= ", paste("'", gsub("-","", Sys.Date() - 1),"' ",sep=""),
                "GROUP BY SUBSTR(b2.posno,0,6), SUBSTR(b2.flowno,0,8), g.code", sep="")
saleshist = dbGetQuery(dbconn, SQL_cmd)
saleshist = data.table(saleshist)
saleshist = saleshist[,.(storecode = STORECODE, prodcode = PRODCODE, date = DT, qty = DAY_QTY)]
fwrite(saleshist, file = "/xgm/R_scripts/monthly_realloc/input/saleshist.csv")
fwrite(saleshist, file = "/xgm/R_scripts/usrdef_realloc/input/saleshist.csv")

SQL_cmd = paste("SELECT storegid, gdgid, qmqty qty FROM jxcsdrpt WHERE qmqty > 0 AND to_char(fildate, 'YYYY-MM-DD') = ", "'", Sys.Date() - 30, "'", sep="")
jxcsdrpt = dbGetQuery(dbconn, SQL_cmd)
jxcsdrpt = data.table(jxcsdrpt)
jxcsdrpt = jxcsdrpt[,.(storegid = STOREGID, prodgid = GDGID, qty = QTY)]
jxcsdrpt = jxcsdrpt %>% 
  left_join(store_info, by ='storegid') %>% 
  left_join(prod_info, by = 'prodgid') %>% 
  data.table()
jxcsdrpt = jxcsdrpt[,.(storecode, catecode, prodcode, qty)]
fwrite(jxcsdrpt, file = "/xgm/R_scripts/monthly_realloc/input/jxcsdrpt.csv")
fwrite(jxcsdrpt, file = "/xgm/R_scripts/usrdef_realloc/input/jxcsdrpt.csv")

#database connect
drv <- JDBC("oracle.jdbc.OracleDriver", classPath="/xgm/jdbc_driver/ojdbc6.jar")
dbconn2 <- dbConnect(drv, "jdbc:oracle:thin:@//192.168.1.41:1521/jdpos", "jidiao", "jidiao")

stopdist = dbGetQuery(dbconn2, 'SELECT * FROM v_jdstopdist')
stopdist = data.table(stopdist)
stopdist = stopdist[,.(storecode = STORECODE, prodcode = GDCODE, source = SOURCE)]
stopdist = stopdist[,.(storecode, prodcode)]
fwrite(stopdist, file = "/xgm/R_scripts/monthly_realloc/input/stopdist.csv")
fwrite(stopdist, file = "/xgm/R_scripts/usrdef_realloc/input/stopdist.csv")

















