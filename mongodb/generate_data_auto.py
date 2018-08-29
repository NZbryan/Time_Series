import pandas as pd
# import numpy as np

def generate_data(input_XS, input_KC):
    ## 销量
    # input_XS=r"F:\xgm_201806\POC\mongo\collections\kucuntest1.csv"
    df1=pd.read_csv(input_XS,index_col=False)
    df1.columns = ['storecode','prodcode','xiaoliang_qty','date']
    # df1['date'] = df1['dt'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
    df1.index = pd.to_datetime(df1['date'])
    df1.index.name = 'xiaoliang_date'
    df1 = df1['2016-01-01':]
    df1.index = df1['date'].astype('str')
    df1.index.name = ['xiaolang_date']
    GZtest_index = list(map(lambda x,y:'X'+x[2:8]+'_'+y[2:12],df1['storecode'],df1['prodcode']))
    df1['GZtest_index'] = GZtest_index
    GZtest_index = list(set(GZtest_index))

    ## 库存

    df2=pd.read_csv(input_KC,index_col=False)
    df2.columns = ['storecode','prodcode','qty','date']
    df2['kucun_index'] = list(map(lambda x,y:'X'+x[2:8]+'_'+y[2:12],df2['storecode'],df2['prodcode']))
    df2['kucun_index_2'] = list(map(lambda x,y:x+'_'+y,df2['date'].tolist(),df2['kucun_index'].tolist()))
    # 去重
    df_kucun = df2
    df_kucun['kucun_qty'] = df_kucun.groupby(['kucun_index_2'])['qty'].transform('sum')
    df_kucun = df_kucun.drop_duplicates(['kucun_index_2'])
    df_kucun.index = df_kucun['date'].astype('str')
    df_kucun = df_kucun.loc[:,['kucun_index','kucun_qty']]
    # data_dict = {}
    data_list = []
    # from progressbar import *
    # progress = ProgressBar()
    from tqdm import tqdm
    # for k in tqdm(range(len(GZtest_index))):

    for k in tqdm(range(3)):
        ## 销量
        dat = df1.loc[df1['GZtest_index'] == GZtest_index[k],['xiaoliang_qty','date']]
        dat = dat.sort_values(by='date')
        # dat.index = dat['date'].tolist()
        date_start = dat['date'].tolist()[0]
        date_end = dat['date'].tolist()[-1]
        date_range = pd.date_range(date_start, date_end).astype('str').tolist()
        true_output = pd.DataFrame(index=date_range)
        data_temp = pd.concat([true_output, dat], axis=1, join_axes=[true_output.index])
        # 库存
        dat_kucun_temp = df_kucun.loc[df_kucun['kucun_index'] == GZtest_index[k]]
        data_temp_merge = pd.concat([data_temp, dat_kucun_temp], axis=1, join_axes=[data_temp.index])
        # data_dict[GZtest_index[k]] = {'qty':dat['qty'].tolist(),'date':dat['date'].astype('str').tolist()}
        # 0-90: 1 , 90- 180:2 ,  180--:3
        data_level = 0
        if dat.shape[0] <=90:  # 判断num的值
            data_level=1
        elif dat.shape[0]>90 & dat.shape[0]<=180:
            data_level=2
        else:
            data_level=3

        data_list.append({'names':GZtest_index[k],
                          'date': data_temp_merge.index.tolist(),
                          'xiaoliang_qty':data_temp_merge['xiaoliang_qty'].tolist(),
                          'kucun_qty':data_temp_merge['kucun_qty'].tolist(),
                          'data_length':[date_start+'~'+date_end,dat.shape[0]/data_temp_merge.shape[0]],
                          'data_level':data_level})


    return(data_list)


input_XS=r'F:\xgm_201806\POC\mongo\collections\kucuntest1.csv'
input_KC=r'F:\xgm_201806\POC\mongo\collections\test_stock.csv'
# output = generate_data(input_XS, input_KC)


# import pymongo
# client = pymongo.MongoClient(host='10.230.0.125', port=27017)
# db = client.python_database
# GZ_data_XL_KC = db.GZ_data_XL_KC
# result_GZtest_data = GZ_data_XL_KC.insert_many(data_list)
# result_GZtest_data.inserted_ids
# GZ_data_XL_KC.estimated_document_count()
#
# GZ_data_XL_KC.find_one({"names":"X002599_0900003932"})


