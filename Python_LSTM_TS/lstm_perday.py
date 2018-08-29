import tensorflow as tf
import pandas as pd
import numpy as np
import os


#######################
# 输入路径与输出路径
#######################
input_path = r'F:\xgm_201806\POC\LSTM_model_test\ModelPath_MultiSKU_oldfile20180724\input_data\train_data.csv'
output_path = 'F:/xgm_201806/POC/LSTM_model_test/test_output/'

#######################
# 代码
#######################
f=open(input_path)
df=pd.read_csv(f,index_col=0)
df = df.T
# test
df = df.iloc[:,:10]

StoreSKU_name = list(df.columns)
# store_name.remove('date')
# Store_name = list(map(lambda x: x[1:],StoreSKU_name))

time_length = df.shape[0]
predict_length = 7
iteration_times = 100

global predict_output
true_output = df.ix[(time_length-predict_length):,].T
predict_output = pd.DataFrame(index = true_output.columns)


# for k in store_name:
#     print('测试数据：', df[k].shape)


for k in StoreSKU_name:
    # k = store_name[1]
    # import gc
    # gc.collect()
    tf.reset_default_graph()
    set_ModelPath = output_path+'ModelPath/model_'+k
    if not os.path.exists(set_ModelPath):
        os.makedirs(set_ModelPath)

    data=np.array(df[k])
    predict_data = data[(len(data)-predict_length):]
    data = data[:(len(data)-predict_length)]
    normalize_data=(data-np.mean(data))/np.std(data)
    normalize_data=normalize_data[:,np.newaxis]

    time_step=7
    rnn_unit=5
    batch_size=100
    input_size=1
    output_size=1
    lr=0.0006
    train_x,train_y=[],[]
    for i in range(len(normalize_data)-time_step-1):
        x=normalize_data[i:i+time_step]
        y=normalize_data[i+1:i+time_step+1]
        train_x.append(x.tolist())
        train_y.append(y.tolist())



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
            predict_output[k] = pred_value.flatten()

    prediction()

predict_output = predict_output.T
predict_output.columns = map(lambda x:'pred_'+x,predict_output.columns)
true_output['sum_7day_true'] = true_output.sum(axis=1).values
predict_output['sum_7day_predict'] = predict_output.sum(axis=1).values
outp_data = pd.concat([true_output,predict_output],axis=1,join='inner')
outp_data['mape'] = (outp_data['sum_7day_true'] - outp_data['sum_7day_predict'])/outp_data['sum_7day_true']
outp_data.insert(loc=0,column='GOODS_code', value=list(map(lambda x:x[8:],outp_data.index)))
outp_data.insert(loc=0,column='STORE_code', value=list(map(lambda x:x[1:7],outp_data.index)))

outp_file = os.path.join(output_path, 'output.csv')
outp_data.to_csv(outp_file)