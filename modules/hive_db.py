# -*- coding: utf-8 -*-
from pyhive import hive
import pandas as pd
import time
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'


class HiveData:
    host = ''
    port = ''
    auth = ''
    database = ''
    username = ''

    def __init__(self, host, port, auth, database, username):
        # 连接sql数据库
        self.host = host
        self.port = port
        self.auth = auth
        self.database = database
        self.username = username


    def conn(self):
        try:
            # 服务器名, 端口，认证、数据库名、用户名
            connect = hive.Connection(host=self.host,
                                        port=self.port,
                                        auth=self.auth,
                                        database=self.database,
                                        username=self.username
                                        )
            # print("Hive连接成功!")
        except Exception as e:
            print("Error:", e)
        return connect

    def select_df(self, sql_select):
        """
        # 定义函数：select_df()根据sql查询语句读取connect连接数据库数据，返回Dataframe类型的数据集
        :param con:数据库的连接，可以接受MySQL、SQL Server和Oracle数据库的连接
        :param sql_select:SQL查询语句
        :return:返回Dataframe类型的数据集
        """
        con = self.conn()
        df_sql = pd.read_sql(sql=sql_select, con=con)
        con.close()  # 关闭数据库连接
        return df_sql

    def select_df_v2(self, sql_select):
        try:
            # 连接数据库
            connect = self.conn()
            # 获取cursor游标：
            cursor = connect.cursor()
            cursor.execute(sql_select)
            # 获取全部数据
            data = cursor.fetchall()
            # 获取列名
            cols = cursor.description
            # 关闭数据库连接
            connect.close()
            # 将数据truple转换为DataFrame
            col = []
            for i in cols:
                col.append(i[0])
            data = list(map(list, data))
            data = pd.DataFrame(data, columns=col)

        except Exception as e:
            data = pd.DataFrame()
            print("Hive数据库连接不成功! Error: ", e)
        return data
    
    def select_df_cursor(self, sql_select, env_set):
        # print(env_set)
        try:
            # 连接数据库
            con = self.conn()
            # 获取cursor游标：
            cursor = con.cursor()
            sqllist = env_set.replace('\t', '').replace('\n', '').split(';')
            for sql_i in sqllist:
                if sql_i.strip():
                    # print(sql_i.strip())
                    cursor.execute(sql_i.strip())
            cursor.execute(sql_select)
            # 获取全部数据
            data = cursor.fetchall()
            # 获取列名
            cols = cursor.description
            # 关闭数据库连接
            con.close()
            # 将数据truple转换为DataFrame
            col = []
            for i in cols:
                col.append(i[0])
            data = list(map(list, data))
            data = pd.DataFrame(data, columns=col)
        except Exception as e:
            data = pd.DataFrame()
            print("Hive数据库连接不成功! Error: ", e)
        return data

    def select_df_cursor_time(self, sql_select, env_set):
        # print(env_set)
        try:
            # 连接数据库
            print('starting...', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            con = self.conn()
            print('连接hive...', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            # 获取cursor游标：
            cursor = con.cursor()
            sqllist = env_set.replace('\t', '').replace('\n', '').split(';')
            for sql_i in sqllist:
                if sql_i.strip():
                    # print(sql_i.strip())
                    cursor.execute(sql_i.strip())
            print('执行set设置', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            cursor.execute(sql_select)
            print('执行sql语句', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            # 获取全部数据
            data = cursor.fetchall()
            print('获取数据', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            # 获取列名
            cols = cursor.description
            print('获取列名', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            # 关闭数据库连接
            con.close()
            print('关闭hive连接', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            # 将数据truple转换为DataFrame
            col = []
            for i in cols:
                col.append(i[0])
            data = list(map(list, data))
            data = pd.DataFrame(data, columns=col)
            print('转化为DataFrame', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        except Exception as e:
            data = pd.DataFrame()
            print("Hive数据库连接不成功! Error: ", e)
        return data
    def close(self, con):
        print('关闭Hive连接')
        con.close()
