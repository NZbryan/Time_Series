#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 001
# 2018.09.13
# python in Spark

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from pyspark_settings import *

def generate_data(sale_hdfs_path,store_path):

    spark = SparkSession.builder.\
    master("local[*]").\
    appName("spark_df").\
    config("spark.some.config.option", "some-value").\
    getOrCreate()
    
    # sale qty
    try:
        salesfile = "hdfs://10.230.0.11:8020"+sale_hdfs_path
        sale_df = spark.read.parquet(salesfile)
    except:
        salesfile = "hdfs://10.230.0.10:8020"+sale_hdfs_path
        sale_df = spark.read.parquet(salesfile)

    sele_col=["store_id","good_gid","date","fill_sale_qty"]
    sale_df = sale_df.select(*sele_col)
    # set_name = ["store_id","good_gid","date","fill_sale_qty"]
    # sale_df = sale_df.toDF(*set_name)
    #94626013

    from pyspark.sql.functions import lpad
    # df22 = sale_df.withColumn("store_id", lpad(sale_df['store_id'],6,'0')).withColumn("good_gid", sale_df['good_gid'].cast('string')).cache()
    df22 = sale_df.withColumn("store_id", lpad(sale_df['store_id'],6,'0')).withColumn("good_gid", sale_df['good_gid'].cast('string'))


    # store

    try:
        storfile = "hdfs://10.230.0.11:8020"+store_path
        store_df = spark.read.parquet(storfile)
    except:
        storefile = "hdfs://10.230.0.10:8020"+store_path
        store_df = spark.read.parquet(storefile)

    # store_path = "/home/test/nby/data_dir/storeinfo.csv"
    # store_df = spark.read.format("com.databricks.spark.csv").option("inferSchema","true").option("header","true").load(store_path)
    store_col = ["store_code","wh_name","alcscm"]
    store_df = store_df.select(*store_col)
    store_set =  ["store_id","warename","ALCSCM"]
    store_df = store_df.toDF(*store_set)
    from pyspark.sql import functions as F
    store_df = store_df.withColumn('ALCSCM',F.when(store_df['ALCSCM'] == '-','01').otherwise(store_df['ALCSCM']))

    store_df = store_df.withColumn("store_id", lpad(store_df['store_id'],6,'0'))
    df22_join = df22.join(store_df,df22.store_id == store_df.store_id,"inner").drop(store_df.store_id)


    from pyspark.sql.functions import concat, col, lit
    from pyspark.sql.functions import year, month, dayofmonth
    df22_join = df22_join.withColumn("date",concat(year("date"), lit("-"), lpad(month("date"),2,'0')))

    # 89847788


    # 3148347
    # count store
    df22_join_uniq = df22_join.dropDuplicates(['store_id','good_gid','date'])
    # 14467
    df22_join_uniq_gb = df22_join_uniq.groupBy("good_gid","date","ALCSCM").count()
    # sum qty
    df22_join_sum = df22_join.groupBy("good_gid","date","ALCSCM","warename").sum("fill_sale_qty").withColumnRenamed('sum(fill_sale_qty)', 'sum_sale_qty')

    # df22_join_uniq_gb.filter("good_gid =='30018144' AND date =='2017-09'").show()
    df22_join_storecount = df22_join_sum.alias("a").join(df22_join_uniq_gb.alias("b"),
        (df22_join_sum.good_gid == df22_join_uniq_gb.good_gid) & 
        (df22_join_sum.ALCSCM == df22_join_uniq_gb.ALCSCM) & 
        (df22_join_sum.date == df22_join_uniq_gb.date)
        ,"left_outer").select("a.good_gid","a.date","a.ALCSCM","a.warename","a.sum_sale_qty","b.count")
    # df22_join.groupBy("good_gid","date","ALCSCM").agg(sum("fill_sale_qty").alias("sum_sale_qty"),func.countDistinct("good_gid","date","ALCSCM")).show()
    sale_data_top100 = df22_join_storecount.toPandas()
    sale_data_top100['good_gid'] = sale_data_top100['good_gid'].astype(str)
    sale_data_top100 = sale_data_top100.sort_values(by=['warename','good_gid','date'],ascending=True)
    sale_data_top100['ALCSCM']  = sale_data_top100['ALCSCM'].astype(str).map(lambda x: x.zfill(2))
    sale_data_top100['sale_qty_perstore'] = sale_data_top100['sum_sale_qty']/sale_data_top100['count']
    sale_data_top100['uniq_ID'] = sale_data_top100['good_gid'] + '_' + sale_data_top100['ALCSCM']
    sale_data_top100 = sale_data_top100.loc[sale_data_top100.ALCSCM != '09',:]

    return sale_data_top100

# df_save.to_csv("/home/test/nby/data_dir/temp_file/sale_fill_na_data.csv",header=True, index=False, encoding='utf-8')

#####################

#####################

def Cal_TB_HB_HOLI(holiday_path,sale_data_top100):
    # holiday
    holiday_month = pd.read_excel(holiday_path, sheet_name="month")
    # holiday_month = pd.read_excel("F:/Sales_Forecast/data_dir/holiday_update201809.xlsx", sheet_name="month")
    holiday_month['month'] = holiday_month['month'].astype(str).map(lambda x: x[:7])
    holiday_month_list = holiday_month['holiday_count_month'].tolist()
    holiday_month_date = holiday_month['month'].tolist()

    # qty
    # sale_data_top100 = pd.read_csv(r'F:\Sales_Forecast\data_dir\top100\sale_fill_na_data.csv')
    # sale_data_top100 = pd.read_csv('/home/test/nby/data_dir/temp_file/sale_fill_na_data.csv')

    loop_count=0
    HB_temp = []
    TB_temp = []
    holiday_data_temp = []
    for name, group in sale_data_top100.groupby('uniq_ID',sort=False):
        # print(name)
        temp_data = pd.DataFrame(
            {'date': group['date'].tolist(), 
            'sale_qty_perstore': group['sale_qty_perstore'].tolist()})
        temp_data['HB_temp'] = temp_data['sale_qty_perstore'] / temp_data['sale_qty_perstore'].shift(1)
        temp_data['TB_temp'] = temp_data['sale_qty_perstore'] / temp_data['sale_qty_perstore'].shift(12)
        temp_data = temp_data.fillna(1)
        date_temp = temp_data['date'].tolist()
        idx_min = holiday_month_date.index(date_temp[0])
        idx_max = holiday_month_date.index(date_temp[-1])+1
        HB_temp.extend(temp_data['HB_temp'].tolist())
        TB_temp.extend(temp_data['TB_temp'].tolist())
        holiday_data_temp.extend(holiday_month_list[idx_min:idx_max])
        loop_count = loop_count+1
        print('loop_count:',loop_count)

    return HB_temp,TB_temp,holiday_data_temp


def main():


    sale_data_top100 = generate_data(sale_hdfs_path,store_path)
    HB_temp,TB_temp,holiday_data_temp = Cal_TB_HB_HOLI(holiday_path,sale_data_top100)
    sale_data_top100['HB_temp'] = HB_temp
    sale_data_top100['TB_temp'] = TB_temp
    sale_data_top100['holiday_data'] = holiday_data_temp
    from calendar import monthrange
    sale_data_top100['days_count'] = sale_data_top100['date'].map(lambda x:monthrange(int(x[:4]),int(x[5:]))[1])
    sale_data_top100['sale_qty_perstore_perdays'] = sale_data_top100['sale_qty_perstore']/sale_data_top100['days_count']
    sale_data_top100.to_csv(month_OUTPUT_path,header=True, index=False, encoding='utf-8')
    if(sale_data_top100.shape[0]>0):
        print("Monthly data output succeed!")

if __name__ == '__main__':
    main()
