#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 001
# 2018.09.13
# python in Spark

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from pyspark_settings import *

def generate_day_data(sale_hdfs_path,store_path):

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
	df22 = sale_df.withColumn("store_id", lpad(sale_df['store_id'],6,'0')).\
	withColumn("good_gid", sale_df['good_gid'].cast('string'))


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
	store_df = store_df.withColumn('ALCSCM',F.\
		when(store_df['ALCSCM'] == '-','01').\
		otherwise(store_df['ALCSCM']))

	store_df = store_df.withColumn("store_id", lpad(store_df['store_id'],6,'0'))
	df22_join = df22.join(store_df,
		df22.store_id == store_df.store_id,
		"inner").drop(store_df.store_id)

	from pyspark.sql.functions import concat, col, lit
	from pyspark.sql.functions import year, month, dayofmonth,date_format
	df22_join = df22_join.withColumn("date",  date_format(col("date"), "yyyy-MM-dd"))
	# sum qty
	df22_join_sum = df22_join.groupBy("good_gid","date","ALCSCM","warename").\
	sum("fill_sale_qty").\
	withColumnRenamed('sum(fill_sale_qty)', 'sum_sale_qty')

	sale_data_top100 = df22_join_sum.toPandas()
	sale_data_top100 = sale_data_top100.\
	sort_values(by=['warename','good_gid','date'],ascending=True)
	sale_data_top100 = sale_data_top100.loc[sale_data_top100.ALCSCM != '09',:]
	#####################
	# holiday_month = sqlContext.read \
	#     .format("com.crealytics.spark.excel") \
	#     .option("location", "/SAS/data_v1/holiday_update201809.xlsx") \
	#     .option("useHeader", "true") \
	#     .option("treatEmptyValuesAsNulls", "true") \
	#     .option("inferSchema", "true") \
	#     .option("timestampFormat", "MM-dd-yyyy")  \
	#     .option("addColorColumns", "False") \
	#     .load()

	#####################

	# holiday
	holiday_month = pd.read_excel(holiday_path, 
		sheet_name="day").rename(columns={'encode':'holiday_day'})
	holiday_month['date'] = holiday_month['date'].astype(str)
	holiday_month = holiday_month.loc[:,['date','holiday_day']]
	sale_data_top100 = sale_data_top100.merge(holiday_month,
		left_on = 'date', right_on = 'date', how = 'left')
	# qty
	# sale_data_top100.to_csv("/SAS/nby/data_dir/model_merge/temp_file/sale_data_top100_day.csv",header=True, index=False, encoding='utf-8')
	sale_data_top100.to_csv(day_OUTPUT_path,header=True, index=False, encoding='utf-8')

	if(sale_data_top100.shape[0]>0):
		print("Daily data output succeed!")



def main():
    generate_day_data(sale_hdfs_path,store_path)

if __name__ == '__main__':
	main()
