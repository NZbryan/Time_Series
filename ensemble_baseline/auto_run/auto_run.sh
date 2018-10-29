#!/bin/bash
/home/test/yudavid/bigdata/spark-2.3.1-bin-hadoop2.7/bin/spark-submit `pwd`/month_pyspark_autov2.py && /home/test/yudavid/bigdata/spark-2.3.1-bin-hadoop2.7/bin/spark-submit `pwd`/day_pyspark_autov2.py
retval=$?
if [ $retval -ne 0 ]; then
    echo "(shell) ERROR , Data preprocessing failed ! retval: $retval"
else
    echo "(shell) Data preprocessing succeed"
    echo "(shell) Then run the model"
    /usr/bin/Rscript `pwd`/model_merge_forecast_v3_autorun.R
    retval_Monthly=$?
    if [ $retval_Monthly -ne 0 ]; then
        echo "(shell) ERROR ,Monthly prediction model failed ! retval: $retval_Monthly"
    else
        echo "(shell) Monthly prediction model succeed ! retval: $retval_Monthly"

    fi

    /usr/bin/Rscript `pwd`/model_merge_forecast_v1_autorun_perday.R
    retval_Daily=$?
    if [ $retval_Daily -ne 0 ]; then
        echo "(shell) ERROR ,Daily prediction model failed ! retval: $retval_Daily"
    else
        echo "(shell) Daily prediction model succeed ! retval: $retval_Daily"

    fi

fi
