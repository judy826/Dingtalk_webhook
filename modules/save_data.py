# python 3.7
# -*-coding:utf-8 -*-
import os
import pymysql
import sqlalchemy
import pandas as pd
from sqlalchemy import MetaData, Table, orm, Column, BLOB, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def create_table():
    """
    创建符合要求的mysql数据表，用于存储数据,不到万不得已，不执行
    """
    conn = pymysql.connect(host='10.50.140.105'  # 连接名称，默认127.0.0.1
                           , user='data_analyze'  # 用户名
                           , passwd='1234.Com'  # 密码
                           , port=3306  # 端口，默认为3306
                           , db='yyx'  # 数据库名称
                           , charset='utf8'  # 字符编码
                           )
    cur = conn.cursor()  # 生成游标对象
    cur.execute('DROP TABLE IF EXISTS yyx.DingTalk_photo')
    sql = """
        create table yyx.DingTalk_photo(
            search_date varchar(50) COMMENT '简报日期',
            photo blob COMMENT '图片信息',
            create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
        )"""  # SQL语句
    cur.execute(sql)  # 执行SQL语句
    cur.close()  # 关闭游标
    conn.close()  # 关闭连接


class DingTalk_photo(Base):
    __tablename__ = 'DingTalk_photo'
    search_date = Column(String(50), primary_key=True)
    photo = Column(BLOB)


def save_data(search_date, photo):
    engine = sqlalchemy.create_engine('mysql+pymysql://data_analyze:1234.Com@10.50.140.105:3306/yyx?charset=utf8mb4')
    data = DingTalk_photo(search_date=search_date, photo=photo)
    Session = orm.sessionmaker(bind=engine)
    session = Session()
    session.add(data)
    session.commit()
    session.close()
    # 删除数据
    abs_path = os.getcwd()
    photo_path = abs_path + '//截图//{}.png'.format(search_date)
    excel_path = abs_path + '//数据填充文件//{}.xlsx'.format(search_date)
    print(photo_path, excel_path)
    if os.path.exists(photo_path):
        os.remove(photo_path)
        print('临时截图删除成功！')
    if os.path.exists(excel_path):
        os.remove(excel_path)
        print('临时文件删除成功！')




def select_data(search_date):
    engine = sqlalchemy.create_engine('mysql+pymysql://data_analyze:1234.Com@10.50.140.105:3306/yyx?charset=utf8mb4')
    sql = """select * from yyx.dingtalk_photo where search_date = '{search_date}' order by create_time desc 
    limit 1""".format(search_date=search_date)
    df = pd.read_sql(sql, engine)
    return df


if __name__ == '__main__':
    search_date_new = '2022-05-25'
    df_select = select_data(search_date_new)
    print(len(df_select))
