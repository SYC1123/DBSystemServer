from socket import *
from time import ctime
import pymssql

from SQLServer import SQLServer


def initDataBase():
    '''
    初始化数据库，建表
    :return:
    '''
    # server    数据库服务器名称或IP
    # user      用户名
    # password  密码
    # database  数据库名称
    connect = pymssql.connect('(local)', 'sa', '123456', 'DBSystem')  # 服务器名,账户,密码,数据库名
    if connect:
        cursor = connect.cursor()  # 创建一个游标对象,python里的sql语句都要通过cursor来执行
        # 建表语句
        sql = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_user'
             )
        begin
        create table tab_user
        (
        Address varchar(30) not null,
        Name varchar(10) not null,
        Tel char(11) not null,
        Udate date not null,
        Password varchar(6) not null,
        Status int not null,
        Primary key(Tel)
        );
        end
        '''
        sql1 = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_reservation'
             )
        begin
        Create table tab_reservation
        (
        Rid	char(11) not null,
        Rplaceid varchar(8) not null,
        Rdate date not null,
        Rtime int not null,
        Rmoney float,
        Primary key(Rid,Rdate)
        );
        end
        '''
        sql2 = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_place'
             )
        begin
        Create table tab_place
        (
        Pid	varchar(8) not null,
        Pstatus	int not null,
        Primary key(Pid)
        );
        end
        '''
        sql3 = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_category'
             )
        begin
        Create table tab_category
        (
        Cid	int not null,
        Cname	varchar(10) not null,
        Primary key(Cid)
        );
        end
        '''
        sql4 = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_img'
             )
        begin
        Create	table tab_img
        (
        Iid	varchar(10) not null,
        Ipalceid varchar(8) not null,
        Iphoto	 image,
        Primary key(Iid)
        );
        end
        '''
        sql5 = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_favorite'
             )
        begin
        Create table tab_favorite
        (
        Uid	char(11) not null,
        Pid	varchar(8) not null,
        Fdate date not null,
        Primary key(Uid,Pid)
        );
        end
        '''
        sql8 = '''
        if not exists(select null from sysobjects where xtype='u' 
                and name ='tab_category_place'
             )
        begin
        Create table tab_category_place
        (
        Pid	 varchar(8) not null,
        Cid		int not null,
        Primary key(Cid,Pid)
        );
        end
        '''
        # 建立视图
        sql6 = '''
        if exists (select * from sysobjects where name = 'View_Order')
        drop view View_Order
        go
        create view View_Order 
        as
        select tab_user.Name,tab_reservation.* from tab_user,tab_reservation where tab_user.Tel=tab_reservation.Rid
        '''
        # 外键约束
        foreignsql1 = "ALTER TABLE tab_place ADD CONSTRAINT FK_place_category FOREIGN KEY(Pcategoryid) REFERENCES tab_category(Cid);"

        foreignsql2 = "ALTER TABLE tab_reservation ADD CONSTRAINT FK_reservation_place FOREIGN KEY(Rplaceid) REFERENCES tab_place(Pid);"

        foreignsql3 = "ALTER TABLE tab_img ADD CONSTRAINT FK_img_place FOREIGN KEY(Ipalceid) REFERENCES tab_place(Pid);"

        foreignsql4 = "ALTER TABLE tab_favorite ADD CONSTRAINT FK_favorite_place FOREIGN KEY(Pid) REFERENCES tab_place(Pid);"

        foreignsql5 = "ALTER TABLE tab_favorite ADD CONSTRAINT FK_favorite_user FOREIGN KEY(Uid) REFERENCES tab_user(Tel);"
        # 存储过程
        sql7 = '''
        if (exists (select * from sys.objects where name = 'getAllOrder'))
        drop proc getAllOrder
        go
        create procedure getAllOrder
        as
        select * from tab_reservation;
        '''
        cursor.execute(sql)
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.execute(sql3)
        cursor.execute(sql4)
        cursor.execute(sql5)
        cursor.execute(sql8)
        # cursor.execute(sql6)
        # cursor.execute(sql7)
        # cursor.execute(foreignsql1)
        # cursor.execute(foreignsql2)
        # cursor.execute(foreignsql3)
        # cursor.execute(foreignsql4)
        # cursor.execute(foreignsql5)

        # 存储过程使用
        # cursor.execute('exec getAllOrder;')
        # row = cursor.fetchone()  # 读取查询结果
        # while row:
        #     print(row)
        #     row = cursor.fetchone()  # 读取查询结果

        connect.commit()
        cursor.close()
        connect.close()


def register(info, sqlserver):
    '''
    :param info: 注册信息
    :param sqlserver: 数据库助手
    :return: 注册结果
    '''
    info=info.split("&")
    print(info)
    # response=sqlserver.ExecQuery('select * from')

if __name__ == '__main__':
    initDataBase()
    sqlserver = SQLServer('(local)', 'sa', '123456', 'DBSystem')
    # 1 定义域名和端口号
    HOST, POST = '', 6666

    # 2 定义缓冲区大小 缓存输入或输出的大小，为了解决速度不匹配的问题
    BUFFER_SIZE = 1024
    ADDR = (HOST, POST)

    # 3 创建服务器的套接字 AF_INET:IPV4 SOCK_STREAM:协议
    tcpServerSocket = socket(AF_INET, SOCK_STREAM)

    # 4 绑定域名和端口号
    tcpServerSocket.bind(ADDR)

    # 5 监听连接，最大连接数一般默认5，如果服务器高并发则增大
    tcpServerSocket.listen(5)  # 被动等待连接
    print('服务器创建成功，等待客户端连接。。。。。')
    # 6 定义一个循环 目的：等待客户端的连接
    while True:
        # 6.1 打开一个客户端对象 同意你连接
        tcpCilentSocket, addr = tcpServerSocket.accept()
        print('连接服务器的客户端对象', addr)

        # 6.2循环过程
        while True:
            # 6.3拿到数据recv()从缓冲区读取指定长度的数据
            # decode(）解码bytes——>str  encode()——>编码 str——>bytes
            data = tcpCilentSocket.recv(BUFFER_SIZE).decode()
            result = None
            if not data:
                break
            print('data=', data)
            command = data[:data.find(':')]
            if command == 'register':
                info = data[data.find(':') + 1:]
                result = register(info, sqlserver)
            # 6.4 发送时间还有信息
            tcpCilentSocket.send("123".encode())
        # 7 关闭资源
        tcpCilentSocket.close()
    tcpServerSocket.close()
