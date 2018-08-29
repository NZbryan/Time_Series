import pymongo
import pandas as pd
import numpy as np
import tensorflow as tf
import os

# hyperparameters
block=1

date_end = '2018-07-23'
# time_length = df.shape[0]
predict_length = 7
iteration_times = 100


time_step = 7
rnn_unit = 5
# batch_size = 100
input_size = 1
output_size = 1
lr = 0.0006


#######################
# 输入数据与输出路径
#######################
client = pymongo.MongoClient(host='10.230.0.125', port=27017)
# 所有记录
all_record = list(client.python_database.GZ_data_XL_KC.find())

output_path = 'F:/xgm_201806/POC/LSTM_model_20180731/per_day/output_20180803/'+'block'+str(block)+'/'

#######################
# 代码
#######################



global predict_output
date_end_7 = pd.to_datetime(date_end) + pd.DateOffset(days=predict_length-1)
date_end_7 = date_end_7.strftime("%Y-%m-%d")
predict_output = pd.DataFrame(index = pd.date_range(date_end,date_end_7).astype('str'))

true_output = pd.DataFrame(index = pd.date_range(date_end,date_end_7).astype('str'))
true_11 = pd.DataFrame(index = pd.date_range(date_end,date_end_7).astype('str'),data=None)
for k in range(len(all_record)):
    # df_temp = pd.DataFrame(GZAnalysisData['X000941_0700047341'],index = pd.to_datetime(GZAnalysisData['X000941_0700047341']['date']))
    df_temp = pd.DataFrame({'date':all_record[k]['date'],'qty':all_record[k]['fill_sale_qty']},index = pd.to_datetime(all_record[k]['date']))
    df_temp2 = df_temp[date_end:date_end_7]['qty']
    df_temp2.index = df_temp2.index.astype('str').tolist()
    true_output[all_record[k]['names']] = pd.concat([true_11, df_temp2], axis=1, join_axes=[true_11.index])['qty'].values
true_output = true_output.T

# length_data = len(GZAnalysisData_index)

# list(range(110*(j-1),110*j))
for k in range(110*(block-1),110*block):
    # k = store_name[1]
    # import gc
    # gc.collect()
    tf.reset_default_graph()
    set_ModelPath = output_path+'ModelPath/model_'+all_record[k]['names']
    if not os.path.exists(set_ModelPath):
        os.makedirs(set_ModelPath)

    # data=np.array(df[k])
    date_list_temp = all_record[k]['date']
    if date_end in date_list_temp:
        index_point = date_list_temp.index(date_end)
        data = np.array(all_record[k]['fill_sale_qty'][:index_point])
    else:
        # 如果没有date_end 这一天，添加，然后定位
        date_list_temp.append(date_end)
        date_list_temp.sort()
        index_point = date_list_temp.index(date_end)
        data = np.array(all_record[k]['fill_sale_qty'][:index_point])

    # predict_data = data[(len(data)-predict_length):]
    # data = data[:(len(data)-predict_length)]
    normalize_data=(data-np.mean(data))/np.std(data)
    normalize_data=normalize_data[:,np.newaxis]


    train_x,train_y=[],[]
    for i in range(len(normalize_data)-time_step-1):
        x=normalize_data[i:i+time_step]
        y=normalize_data[i+1:i+time_step+1]
        train_x.append(x.tolist())
        train_y.append(y.tolist())

    # batch_size=100
    batch_size = int(len(train_x)/2)


    # ——————————————————定义神经网络变量——————————————————
    X = tf.placeholder(tf.float32, [None, time_step, input_size])  # 每批次输入网络的tensor
    Y = tf.placeholder(tf.float32, [None, time_step, output_size])  # 每批次tensor对应的标签
    # 输入层、输出层权重、偏置
    weights = {
        'in': tf.Variable(tf.random_normal([input_size, rnn_unit])),
        'out': tf.Variable(tf.random_normal([rnn_unit, 1]))
    }
    biases = {
        'in': tf.Variable(tf.constant(0.1, shape=[rnn_unit, ])),
        'out': tf.Variable(tf.constant(0.1, shape=[1, ]))
    }


    # ——————————————————定义神经网络变量——————————————————
    def lstm(batch):  # 参数：输入网络批次数目
        w_in = weights['in']
        b_in = biases['in']
        input = tf.reshape(X, [-1, input_size])  # 需要将tensor转成2维进行计算，计算后的结果作为隐藏层的输入
        input_rnn = tf.matmul(input, w_in) + b_in
        input_rnn = tf.reshape(input_rnn, [-1, time_step, rnn_unit])  # 将tensor转成3维，作为lstm cell的输入
        cell = tf.nn.rnn_cell.BasicLSTMCell(rnn_unit)
        init_state = cell.zero_state(batch, dtype=tf.float32)
        output_rnn, final_states = tf.nn.dynamic_rnn(cell, input_rnn, initial_state=init_state,
                                                     dtype=tf.float32)  # output_rnn是记录lstm每个输出节点的结果，final_states是最后一个cell的结果
        output = tf.reshape(output_rnn, [-1, rnn_unit])  # 作为输出层的输入
        w_out = weights['out']
        b_out = biases['out']
        pred = tf.matmul(output, w_out) + b_out
        return pred, final_states


    # ——————————————————训练模型——————————————————
    def train_lstm():
        global batch_size
        pred, _ = lstm(batch_size)
        # 损失函数
        loss = tf.reduce_mean(tf.square(tf.reshape(pred, [-1]) - tf.reshape(Y, [-1])))
        # loss = tf.reduce_mean(tf.abs(tf.divide(tf.subtract(tf.reshape(pred, [-1]), tf.reshape(Y, [-1])), (tf.reshape(Y, [-1]) + 1e-10))))

        train_op = tf.train.AdamOptimizer(lr).minimize(loss)
        saver = tf.train.Saver(tf.global_variables())
        with tf.Session() as sess:
            sess.run(tf.global_variables_initializer())
            # 重复训练10000次
            for i in range(iteration_times):
                step = 0
                start = 0
                end = start + batch_size
                while (end < len(train_x)):
                    _, loss_ = sess.run([train_op, loss], feed_dict={X: train_x[start:end], Y: train_y[start:end]})
                    start += batch_size
                    end = start + batch_size
                    # 每10步保存一次参数
                    if step % 10 == 0:
                        print(i, step, loss_)
                        print("保存模型：", saver.save(sess,
                                                  set_ModelPath+ '/stock.model'))
                    step += 1

    train_lstm()


    tf.reset_default_graph()
    # ——————————————————定义神经网络变量——————————————————
    X = tf.placeholder(tf.float32, [None, time_step, input_size])  # 每批次输入网络的tensor
    Y = tf.placeholder(tf.float32, [None, time_step, output_size])  # 每批次tensor对应的标签
    # 输入层、输出层权重、偏置
    weights = {
        'in': tf.Variable(tf.random_normal([input_size, rnn_unit])),
        'out': tf.Variable(tf.random_normal([rnn_unit, 1]))
    }
    biases = {
        'in': tf.Variable(tf.constant(0.1, shape=[rnn_unit, ])),
        'out': tf.Variable(tf.constant(0.1, shape=[1, ]))
    }
    def prediction():
        pred,_=lstm(1)      #预测时只输入[1,time_step,input_size]的测试数据
        saver=tf.train.Saver(tf.global_variables())
        with tf.Session() as sess:
            #参数恢复
            module_file = tf.train.latest_checkpoint(set_ModelPath)
            saver.restore(sess, module_file)

            #取训练集最后一行为测试样本。shape=[1,time_step,input_size]
            prev_seq=train_x[-1]
            predict=[]
            #得到之后100个预测结果
            for i in range(predict_length):
                next_seq=sess.run(pred,feed_dict={X:[prev_seq]})
                predict.append(next_seq[-1])
                #每次得到最后一个时间步的预测结果，与之前的数据加在一起，形成新的测试样本
                prev_seq=np.vstack((prev_seq[1:],next_seq[-1]))
            #以折线图表示结果
            # plt.figure()
            # plt.plot(list(range(len(normalize_data))), normalize_data, color='b')
            # plt.plot(list(range(len(normalize_data), len(normalize_data) + len(predict))), predict, color='r')
            # plt.show()
            pred_value = np.array(predict) * np.std(data) + np.mean(data)
            predict_output[all_record[k]['names']] = pred_value.flatten()

    prediction()

predict_output = predict_output.T
predict_output.columns = map(lambda x:'pred_'+x,predict_output.columns)
true_output['sum_7day_true'] = true_output.sum(axis=1).values
predict_output['sum_7day_predict'] = predict_output.sum(axis=1).values
outp_data = pd.concat([true_output,predict_output],axis=1,join='inner')
outp_data['mape'] = (outp_data['sum_7day_true'] - outp_data['sum_7day_predict'])/outp_data['sum_7day_true']
outp_data.insert(loc=0,column='GOODS_code', value=list(map(lambda x:x[8:],outp_data.index)))
outp_data.insert(loc=0,column='STORE_code', value=list(map(lambda x:x[1:7],outp_data.index)))

outp_file = os.path.join(output_path, 'block'+str(block)+'_output.csv')
outp_data.to_csv(outp_file)