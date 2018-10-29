#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings

sale_hdfs_path = "/user/hive/warehouse/datamodel.db/data_for_model_sep_par"
store_path = "/user/hive/warehouse/dim_gn.db/dim_stores"
holiday_path = "./input_output_file/holiday_update201809.xlsx"
month_OUTPUT_path = "./input_output_file/sale_data_top100_month.csv"
day_OUTPUT_path = "./input_output_file/sale_data_top100_day.csv"




# sale_hdfs_path = "/user/hive/warehouse/datamodel.db/data_for_model_sep_par"
# store_path = "/user/hive/warehouse/dim_gn.db/dim_stores"
# holiday_path = "/SAS/data_v1/holiday_update201809.xlsx"
# month_OUTPUT_path = "/SAS/nby/data_dir/model_merge/output/test_out1/sale_data_top100_month.csv"
