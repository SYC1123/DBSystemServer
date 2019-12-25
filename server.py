from socket import *
from time import ctime
import pymssql
import datetime
import json

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
        Id int Identity(1,1),
        Rid	char(11) not null,
        Rplaceid varchar(8) not null,
        Rdate date not null,
        Rtime int not null,
        Rmoney float,
        Primary key(Id)
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
        Iphoto	 varchar(MAX),
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
    # 注册
    '''
    :param info: 注册信息
    :param sqlserver: 数据库助手
    :return: 注册结果
    '''
    info = info.split("&")
    phone = "'" + info[0] + "'"
    name = "'" + info[1] + "'"
    address = "'" + info[2] + "'"
    password = "'" + info[3] + "'"
    sql = 'select * from tab_user where Tel=%s' % (phone)
    response = sqlserver.ExecQuery(sql)
    if len(response) != 0:
        return '该用户已经注册！'
    else:
        today = "'" + str(datetime.date.today()) + "'"
        sql = '''insert into tab_user(Address,Name,Tel,Udate,Password,Status) values (%s,%s,%s,%s,%s,1)''' % (
            address, name, phone, today, password)
        print(sql)
        sqlserver.ExecNonQuery(sql)
        sql = '''create login [%s] with password=%s, default_database=DBSystem ''' % (info[0], password)
        sqlserver.ExecNonQuery(sql)
        sql = '''create user [%s] for login [%s] with default_schema=dbo''' % (info[0], info[0])
        sqlserver.ExecNonQuery(sql)
        sql = '''GRANT SELECT ON  tab_img TO [%s] ''' % (
            info[0])
        sqlserver.ExecNonQuery(sql)
        sql = '''GRANT SELECT ON  tab_reservation TO [%s] ''' % (
            info[0])
        sqlserver.ExecNonQuery(sql)
        sql = '''GRANT SELECT ON  tab_place TO [%s] ''' % (
            info[0])
        sqlserver.ExecNonQuery(sql)
        sql = '''GRANT SELECT ON  tab_category TO [%s] ''' % (
            info[0])
        sqlserver.ExecNonQuery(sql)
        sql = '''GRANT SELECT ON  tab_category_place TO [%s] ''' % (
            info[0])
        sqlserver.ExecNonQuery(sql)
        sql = '''GRANT SELECT ON tab_favorite TO [%s] ''' % (
            info[0])
        sqlserver.ExecNonQuery(sql)
        return '1'


def login(info, sqlserver):
    '''
    :param info: 登录信息
    :param sqlserver: 数据库助手
    :return: 结果
    '''
    info = info.split("&")
    account = "'" + info[0] + "'"
    password = "'" + info[1] + "'"
    sql = 'select * from tab_user where Tel=%s' % (account)
    response = sqlserver.ExecQuery(sql)
    if len(response) != 0:
        # 该用户存在
        # print(response)
        # print(response[0][4])
        if response[0][4] != info[1]:
            return '2'
        else:
            jsonData = []
            data = {}
            data['Address'] = str(response[0][0])
            data['Name'] = str(response[0][1])
            data['Tel'] = str(response[0][2])
            data['Date'] = str(response[0][3])
            data['Password'] = str(response[0][4])
            data['Status'] = int(response[0][5])
            jsonData.append(data)
            jsondatar = json.dumps(jsonData, ensure_ascii=False)
            return jsondatar[1:len(jsondatar) - 1]
    else:
        return '3'


def queryPlace(info, sqlserver):
    '''
    场地查询
    :param info:查询信息
    :param sqlserver: 数据库助手
    :return: 已经预定的场地号，json数组带key
    '''
    info = "'" + info + "'"
    sql = 'select Rplaceid,Rtime from tab_reservation where Rdate=%s' % (info)
    response = sqlserver.ExecQuery(sql)
    print(response)
    # json数组带key
    jsonDir = {}
    jsonData = []
    for row in response:
        data = {}
        data['PlaceID'] = int(row[0])
        data['OrderTime'] = row[1]
        jsonData.append(data)
        jsondatar = json.dumps(jsonData, ensure_ascii=False)
    jsonDir.update({"OrderedPlace": jsonData})
    sql = 'select Pid from tab_place where Pstatus=0'
    response = sqlserver.ExecQuery(sql)
    jsonData1 = []
    for row in response:
        data = {}
        data['PlaceID'] = int(row[0])
        jsonData1.append(data)
        jsondatar = json.dumps(jsonData1, ensure_ascii=False)
    jsonDir.update({"RepairingPlace": jsonData1})
    print(jsonDir)
    return str(jsonDir)


def queryplacedetail(info, sqlserver):
    '''
    查询场地详情
    :param info:场地号
    :param sqlserver:
    :return: json数据
    '''
    sql = '''select tab_category.Cid from tab_category,tab_category_place 
          where tab_category.Cid=tab_category_place.Cid and tab_category_place.Pid=''' + info
    response = sqlserver.ExecQuery(sql)
    data = {}
    details = []
    for row in response:
        details.append(row[0])
    data['DetailNum'] = len(details)
    count = 0
    for value in details:
        data[str(count)] = value
        count = count + 1
    sql = '''select Iphoto from tab_img
             where Ipalceid=''' + info
    response = sqlserver.ExecQuery(sql)
    data["URL1"] = str(response[0][0])
    data["URL2"] = str(response[1][0])
    jsondatar = json.dumps(data, ensure_ascii=False)
    print(jsondatar)
    return jsondatar


def changeinfo(info, sqlserver):
    '''
    基本信息修改
    :param info:修改信息
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    name = "'" + info[0] + "'"
    tel = "'" + info[1] + "'"
    address = "'" + info[2] + "'"
    password = "'" + info[3] + "'"
    befortel = "'" + info[4] + "'"
    if befortel != tel:
        sql = 'select * from tab_user where Tel=%s' % (befortel)
        response = sqlserver.ExecQuery(sql)
        if len(response) != 0:
            return '该用户已经注册！'
    else:
        sql = "UPDATE tab_user SET Address = %s, Name = %s,Tel = %s,Password= %s WHERE Tel = %s" % (
            address, name, tel, password, befortel)
        sqlserver.ExecNonQuery(sql)
        return '1'


def collect(info, sqlserver):
    '''
    场地收藏
    :param info:
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    uid = "'" + info[0] + "'"
    pid = "'" + info[1] + "'"
    date = "'" + info[2] + "'"
    sql = 'select * from tab_favorite where Uid=%s and Pid=%s' % (uid, pid)
    response = sqlserver.ExecQuery(sql)
    if len(response) != 0:
        return "该场地已收藏！"
    else:
        sql = '''insert into tab_favorite(Uid,Pid,Fdate) values (%s,%s,%s)''' % (uid, pid, date)
        print(sql)
        sqlserver.ExecNonQuery(sql)
        return "收藏成功！"


def reservation(info, sqlserver):
    '''
    场地预定
    :param info:预定信息
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    tel = "'" + info[0] + "'"
    position = "'" + info[1] + "'"
    date = "'" + info[2] + "'"
    time = "'" + info[3] + "'"
    money = "'" + info[4] + "'"
    sql = '''insert into  tab_reservation(Rid,Rplaceid,Rdate,Rtime,Rmoney) values (%s,%s,%s,%s,%s)''' % (
        tel, position, date, time, money)
    print(sql)
    sqlserver.ExecNonQuery(sql)
    return "预定成功！"


def order(info, sqlserver):
    '''
    查看订单
    :param info:
    :param sqlserver:
    :return:json数组
    '''
    info = "'" + info + "'"
    sql = "EXEC GetUserOrder " + info
    response = sqlserver.ExecQuery(sql)
    jsonData = []
    for row in response:
        data = {}
        data['PlaceID'] = int(row[2])
        data['Date'] = row[3]
        data['OrderTime'] = int(row[4])
        data['Money'] = float(row[5])
        jsonData.append(data)
        jsondatar = json.dumps(jsonData, ensure_ascii=False)
    print(str(jsonData))
    return str(jsonData)


def mycollection(info, sqlserver):
    '''
    查询我的收藏
    :param info:用户电话号
    :param sqlserver:
    :return: json数组
    '''
    info = "'" + info + "'"
    sql = "select * from View_Collection where Iphoto in (select min(Iphoto) from View_Collection group by Pid) and Uid=" + info
    response = sqlserver.ExecQuery(sql)
    jsonData = []
    for row in response:
        data = {}
        data['PlaceID'] = int(row[1])
        data['Date'] = row[2]
        data['URL'] = row[3]
        jsonData.append(data)
        jsondatar = json.dumps(jsonData, ensure_ascii=False)
    print(str(jsonData))
    return str(jsonData)


def querybad(info, sqlserver):
    '''
    查询需要维修的场地
    :param info:
    :param sqlserver:
    :return:json数组
    '''
    sql = "select Pid from tab_place where Pstatus=0"
    response = sqlserver.ExecQuery(sql)
    jsonData = []
    for row in response:
        data = {}
        data['PlaceID'] = int(row[0])
        jsonData.append(data)
        jsondatar = json.dumps(jsonData, ensure_ascii=False)
    print(str(jsonData))
    return str(jsonData)


def repair(info, sqlserver):
    '''
    场地维修
    :param info: 场地号
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    print(info)
    for key in info:
        sql = '''
        declare @Sta int=%s
        declare @StaNo int
        declare test_cursor cursor local 
        for select Pstatus from tab_place
        open test_cursor 
        fetch next from test_cursor into @StaNo
        while(@@FETCH_STATUS=0)
        begin
        set @StaNo=1
        begin
            update tab_place set Pstatus=@StaNo where Pid=@Sta
        end
        fetch next from test_cursor into @StaNo
        end
        close test_cursor
        deallocate test_cursor
        ''' % key
        sqlserver.ExecNonQuery(sql)
    return '维修完成！'


def destroy(info, sqlserver):
    '''
    报损
    :param info: 损坏的场地号
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    print(info)
    for key in info:
        sql = '''
            declare @Sta int=%s
            declare @StaNo int
            declare test_cursor cursor local 
            for select Pstatus from tab_place
            open test_cursor 
            fetch next from test_cursor into @StaNo
            while(@@FETCH_STATUS=0)
            begin
            set @StaNo=0
            begin
                update tab_place set Pstatus=@StaNo where Pid=@Sta
            end
            fetch next from test_cursor into @StaNo
            end
            close test_cursor
            deallocate test_cursor
            ''' % key
        sqlserver.ExecNonQuery(sql)
    return '报损完成！'


def report(info, sqlserver):
    '''
    报表
    :param info:
    :param sqlserver:
    :return:
    '''
    sql = "select sum(Rmoney) from tab_reservation"
    response = sqlserver.ExecQuery(sql)
    sql1 = "select count(*) from tab_reservation"
    response1 = sqlserver.ExecQuery(sql1)
    jsonData = []
    data = {}
    data['Money'] = str(response[0][0])
    data['Order'] = str(response1[0][0])
    jsonData.append(data)
    jsondatar = json.dumps(jsonData, ensure_ascii=False)
    print(jsonData)
    return jsondatar[1:len(jsondatar) - 1]


def delete(info, sqlserver):
    '''
    删除收藏
    :param info:
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    id = "'" + info[0] + "'"
    tel = "'" + info[1] + "'"
    sql = "DELETE FROM tab_favorite WHERE Pid = %s and Uid = %s" % (id, tel)
    sqlserver.ExecNonQuery(sql)
    return '删除成功！'


def cancel(info, sqlserver):
    '''
    取消订单
    :param info:
    :param sqlserver:
    :return:
    '''
    info = info.split("&")
    date = "'" + info[0] + "'"
    tel = "'" + info[1] + "'"
    id= "'" + info[2] + "'"
    time= "'" + info[3] + "'"
    sql = "DELETE FROM tab_reservation WHERE Rdate = %s and Rid = %s and Rplaceid = %s and Rtime=%s" % (date, tel,id,time)
    sqlserver.ExecNonQuery(sql)
    return '退订成功！'


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
            elif command == 'login':
                info = data[data.find(':') + 1:]
                result = login(info, sqlserver)
            elif command == 'queryplace':
                info = data[data.find(':') + 1:]
                result = queryPlace(info, sqlserver)
            elif command == 'placedetail':
                info = data[data.find(':') + 1:]
                result = queryplacedetail(info, sqlserver)
            elif command == 'changeinfo':
                info = data[data.find(':') + 1:]
                result = changeinfo(info, sqlserver)
            elif command == 'collect':
                info = data[data.find(':') + 1:]
                result = collect(info, sqlserver)
            elif command == 'reservation':
                info = data[data.find(':') + 1:]
                result = reservation(info, sqlserver)
            elif command == 'order':
                info = data[data.find(':') + 1:]
                result = order(info, sqlserver)
            elif command == 'mycollection':
                info = data[data.find(':') + 1:]
                result = mycollection(info, sqlserver)
            elif command == 'querybad':
                info = data[data.find(':') + 1:]
                result = querybad(info, sqlserver)
            elif command == 'repair':
                info = data[data.find(':') + 1:]
                result = repair(info, sqlserver)
            elif command == 'destroy':
                info = data[data.find(':') + 1:]
                result = destroy(info, sqlserver)
            elif command == 'report':
                info = data[data.find(':') + 1:]
                result = report(info, sqlserver)
            elif command == 'delete':
                info = data[data.find(':') + 1:]
                result = delete(info, sqlserver)
            elif command == 'cancel':
                info = data[data.find(':') + 1:]
                result = cancel(info, sqlserver)
            # 6.4 发送时间还有信息
            tcpCilentSocket.send(result.encode())
        # 7 关闭资源
        tcpCilentSocket.close()
    tcpServerSocket.close()
