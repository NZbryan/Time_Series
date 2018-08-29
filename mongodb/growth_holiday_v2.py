import numpy as np
import pandas as pd
import pymongo


def TB_holiday(type_number):
    temp_df = holiday_df.loc[holiday_df.encode == type_number,['group_date','temp_col']]
    tem_TB = temp_df.groupby(temp_df['group_date']).mean()['temp_col'].values
    if type_number == 10:
        tem_TB = [holiday_df.loc[holiday_df.encode == 10, 'temp_col'][:'2016-01-03'].mean(),
                  holiday_df.loc[holiday_df.encode == 10, 'temp_col']['2016-12':'2017-01-31'].mean(),
                  holiday_df.loc[holiday_df.encode == 10, 'temp_col']['2017-12':].mean()]
    return [round(tem_TB[0],2),round(tem_TB[1]/tem_TB[0],2),round(tem_TB[2]/tem_TB[1],2)]



def HB_holiday_7days_avg(type_number,date_length=7):

    temp_1 = holiday_df.loc[(holiday_df.encode == type_number)&(holiday_df['group_date']=='a2016'),].index
    temp_2 = holiday_df.loc[(holiday_df.encode == type_number)&(holiday_df['group_date']=='a2017'), :].index
    temp_3 = holiday_df.loc[(holiday_df.encode == type_number)&(holiday_df['group_date']=='a2018'), :].index
    if type_number == 10:
        temp_1 = holiday_df.loc[holiday_df.encode == type_number, :][:'2016-01-03'].index
        temp_2 = holiday_df.loc[holiday_df.encode == type_number, :]['2016-12':'2017-01-31'].index
        temp_3 = holiday_df.loc[holiday_df.encode == type_number, :]['2017-12':].index

    avg_2016 = holiday_df.loc[(holiday_df.encode == type_number)&(holiday_df['group_date']=='a2016'),'temp_col'].mean()
    avg_2017 = holiday_df.loc[(holiday_df.encode == type_number)&(holiday_df['group_date']=='a2017'),'temp_col'].mean()
    avg_2018 = holiday_df.loc[(holiday_df.encode == type_number)&(holiday_df['group_date']=='a2018'),'temp_col'].mean()

    temp_1_min = (min(temp_1) + pd.DateOffset(days=-date_length)).strftime("%Y-%m-%d")
    temp_1_max = (min(temp_1) + pd.DateOffset(days=-1)).strftime("%Y-%m-%d")
    temp_2_min = (min(temp_2) + pd.DateOffset(days=-date_length)).strftime("%Y-%m-%d")
    temp_2_max = (min(temp_2) + pd.DateOffset(days=-1)).strftime("%Y-%m-%d")
    temp_3_min = (min(temp_3) + pd.DateOffset(days=-date_length)).strftime("%Y-%m-%d")
    temp_3_max = (min(temp_3) + pd.DateOffset(days=-1)).strftime("%Y-%m-%d")

    # 2016、 17、18 前n天
    front7_avg_2016 = holiday_df[temp_1_min:temp_1_max]['temp_col'].mean()
    front7_avg_2017 = holiday_df[temp_2_min:temp_2_max]['temp_col'].mean()
    front7_avg_2018 = holiday_df[temp_3_min:temp_3_max]['temp_col'].mean()

    HB_data_2016 = round(avg_2016 / front7_avg_2016,2)
    HB_data_2017 = round(avg_2017 / front7_avg_2017,2)
    HB_data_2018 = round(avg_2018 / front7_avg_2018,2)
    return [HB_data_2016,HB_data_2017,HB_data_2018]


def Get_all_TB_HB():
    TB_data = []
    HB_data = []
    for k in range(1,11):
        HB_data.append(HB_holiday_7days_avg(k))
        TB_data.append(TB_holiday(k))
    return HB_data,TB_data

# for k in range(len(sale_data_top100_mongo)):
def insert_mongo(k):
    holiday_df.index = holiday_df['date']
    holiday_df['temp_col'] = np.NaN
    temp_data = pd.DataFrame(
        {'date_raw': sale_data_top100_mongo[k]['date_raw'], 'sale_qty_raw': sale_data_top100_mongo[k]['sale_qty_raw']},
        index=pd.to_datetime(sale_data_top100_mongo[k]['date_raw']))
    temp_data = temp_data['2016-01-01':'2018-08-06']
    holiday_df.loc[temp_data['date_raw'], 'temp_col'] = temp_data['sale_qty_raw']
    holiday_df.index = pd.to_datetime(holiday_df.index)
    HB_data, TB_data = Get_all_TB_HB()



    names = sale_data_top100_mongo[k]['names']
    generate_dict = {'names':names,
                     'store_id':sale_data_top100_mongo[k]['store_id'],
                     'good_id': sale_data_top100_mongo[k]['good_id'],
                     'holiday_code':list(range(1,11)),
                     'holiday_type': ['春节','清明节','五一劳动节','端午节','六一儿童节',
                               '十一国庆节','618购物节','双十一购物节','圣诞节','元旦'],
                     'TB_data_Final': np.around(np.nanmean(TB_data, axis=1),decimals=2).tolist(),
                     'TB_data':TB_data,
                     'HB_data':HB_data,
                     'HB_data_Final': np.around(np.nanmean(HB_data, axis=1),decimals=2).tolist(),
                     }
    Holiday_GrowthRatio_Merge.insert_one(generate_dict)
    print('finsh:',names)

if __name__ == '__main__':

    # holiday_path = r"F:\Sales_Forecast\data_dir\holiday_update20180821.csv"
    holiday_path = '/home/test/nby/data_dir/holiday_update20180821.csv'
    holiday_df = pd.read_csv(holiday_path, encoding='gbk', index_col=0)
    holiday_df.index = pd.to_datetime(holiday_df['date'])
    holiday_df['group_date'] = 0
    holiday_df.loc[:'2016-12-31', 'group_date'] = 'a2016'
    holiday_df.loc['2017-01-01':'2017-12-31', 'group_date'] = 'a2017'
    holiday_df.loc['2018-01-01':, 'group_date'] = 'a2018'
    holiday_df.index = holiday_df['date']

    client = pymongo.MongoClient(host='10.230.0.125', port=27017)
    db = client.python_database
    # db.GZ_data_XL_KC.drop()
    sale_data_top100_clean = db.sale_data_top100_clean
    sale_data_top100_mongo = list(sale_data_top100_clean.find())

    from multiprocessing import Pool
    client2 = pymongo.MongoClient(host='10.230.0.125', port=27017)
    db2 = client2.python_database
    # db.Holiday_GrowthRatio_Merge.drop()
    Holiday_GrowthRatio_Merge = db2.Holiday_GrowthRatio_Merge
    n_cores = 10  # number of splits (logical cores of the CPU-1)
    pool = Pool(n_cores)
    length_list = list(range(len(sale_data_top100_mongo)))
    pool.map(insert_mongo, length_list)  # process data_inputs iterable with pool

