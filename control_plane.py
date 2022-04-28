import time

import pymysql
import pymongo
from bson import ObjectId
import re


def testconnection():
    host1 = ''
    user1 = ''
    password1 = ''
    dbname = 'Data_index'
    db = pymysql.connect(host=host1,
                         user=user1,
                         password=password1,
                         database=dbname)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 使用 execute()  方法执行 SQL 查询
    cursor.execute("SELECT VERSION()")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchone()

    print("Database version : %s " % data)

    # 关闭数据库连接
    db.close()


def init(CIname):
    host1 = ''
    user1 = ''
    password1 = ''
    dbname = 'Control_index'
    db = pymysql.connect(host=host1,
                         user=user1,
                         password=password1,
                         database=dbname)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql2 = 'DROP TABLE IF EXISTS ' + CIname + ';'
    cursor.execute(sql2)
    sql0 = "CREATE TABLE " + CIname + "(id INT(11) NOT NULL AUTO_INCREMENT, time CHAR(20), prefix CHAR(" \
                                      "20), ASN CHAR(10), last_time CHAR(22), last_id CHAR(25), " \
                                      "update_times INT, live_time FLOAT, " \
                                      "reachable_proportion_T1 FLOAT, reachable_proportion_T2 FLOAT, " \
                                      "reachable_proportion_T3 FLOAT, reachable_proportion_T4 FLOAT, " \
                                      "reachable_proportion_T5 FLOAT,reachable_proportion_T6 FLOAT,"\
                                      "reachable_proportion_T7 FLOAT,"\
                                      "reachable_average FLOAT, reachable_variance FLOAT, reachable_time INT, " \
                                      "repeat_announce INT , redundancy_cancel INT, path_consistency CHAR(5), "\
                                      "PRIMARY KEY (`id`) USING BTREE, FULLTEXT KEY `ft_prefix` (`prefix`))"\
                                      "ENGINE=InnoDB;"
    print(sql0)
    cursor.execute(sql0)
    myclient = pymongo.MongoClient('mongodb://10.245.146.64:27017/')
    mydb2 = myclient['Control_IPAS']
    mycol2 = mydb2['IP']
    mgres = mycol2.find_one()["data"]

    i = 0
    li = 0
    for key in mgres.keys():
        i = i + 1
        if i - li == 1000:
            li = i
            print(i)
        sql1 = "insert into " + CIname + '(prefix,ASN,update_times,live_time,repeat_announce,redundancy_cancel,' \
                                         'reachable_proportion_T1,reachable_proportion_T2,reachable_proportion_T3,'\
                                         'reachable_proportion_T4,reachable_proportion_T5,reachable_proportion_T6,'\
                                         'reachable_proportion_T7,time)values("' + key \
               + '","' + mgres[key] + '",0,0,0,-1,0,0,0,0,0,0,0,"' + CIname[2:] + '");'
        cursor.execute(sql1)
        db.commit()
    db.close()


def testing():
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["routeviews_UPDATEs"]
    mycol = mydb["updates_20220316_1315"]
    res = mycol.find_one()
    sql3 = "SELECT id FROM " + "CI202203_03" + ' WHERE prefix="' + res["PREFIX"] + '";'
    host1 = ''
    user1 = ''
    password1 = ''
    dbname = 'Control_index'
    db = pymysql.connect(host=host1,
                         user=user1,
                         password=password1,
                         database=dbname)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor.execute(sql3)
    myres = cursor.fetchone()
    print(myres)


def giveanum(collectionname):
    n = int(collectionname[12:])
    if n < 7:
        m = 1
    elif n < 13:
        m = 2
    elif n < 19:
        m = 3
    else:
        m = 4
    return m


def computetime(t1, t2):
    t = 0
    pat = re.compile(r'\d+:\d+')
    pat1 = re.compile(r'\d+/\d+/\d+')
    a0, b0 = pat.search(t1).span()
    a1, b1 = pat1.search(t1).span()
    a2, b2 = pat.search(t2).span()
    a3, b3 = pat1.search(t2).span()
    mh = 30 * 60
    if int(t2[a1:b1][3:5]) in [1, 3, 5, 7, 8, 10, 12]:
        mh = 31 * 60
    elif int(t2[a1:b1][3:5]) == 2:
        mh = 28 * 60
    t = t + (int(t1[a1:b1][3:5]) - int(t2[a3:b3][3:5])) * mh
    t = t + (int(t1[a1:b1][6:8]) - int(t2[a3:b3][6:8])) * 24
    t = t + (int(t1[a0:b0][:2]) - int(t2[a2:b2][:2]))
    t = t + (int(t1[a0:b0][-2:]) - int(t2[a2:b2][-2:])) / 60
    return t


def giveap(ip):
    ll = {}
    n = 0
    pat = re.compile(r'\d+')
    res = pat.findall(ip)
    if int(res[4]) >= 24:
        i = 32 - int(res[4])
        tmp = 0
        while i < 8:
            tmp = tmp + 2 ** i
            i = i + 1
        for i in range(int(res[3]) & tmp, (int(res[3]) | (2 ** 8 - tmp - 1)) + 1):
            ll[res[0] + '.' + res[1] + '.' + res[2] + '.' + str(i)] = ''
            n = n + 1
    else:
        i = 7
        tmp = 0
        while i >= 24 - int(res[4]):
            tmp = tmp + 2 ** i
            i = i - 1
        for i in range(int(res[2]) & tmp, (int(res[2]) | (2 ** 8 - tmp - 1)) + 1):
            for j in range(0, 256):
                ll[res[0] + '.' + res[1] + '.' + str(i) + '.' + str(j)] = ''
                n = n + 1
    return ll, n


def fromPing(PDname, CIname):
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["Ping"]
    mycol = mydb[PDname]
    mgres = mycol.find_one()["IP"]
    lll = {}
    for i in mgres:
        lll[i] = ''
    host1 = ''
    user1 = ''
    password1 = ''
    dbname = 'Control_index'
    db = pymysql.connect(host=host1,
                         user=user1,
                         password=password1,
                         database=dbname)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    i = 0
    while True:
        if i == 100:
            break
        i = i + 1
        sql3 = "SELECT prefix,reachable_proportion_T1,reachable_proportion_T2,reachable_proportion_T3," \
               "reachable_proportion_T4 FROM " + CIname + " WHERE id=" + str(i) + "; "
        print(sql3)
        cursor.execute(sql3)
        myres = cursor.fetchone()
        ll, n = giveap(myres[0])
        cnt = 0
        for key in ll.keys():
            if key in lll.keys():
                cnt = cnt + 1
        p = cnt / n
        k = 1
        m = giveanum(PDname)
        t = p
        while k < 5:
            if k != m:
                t = t + myres[k]
            k = k + 1
        t = t / 4
        var = (p - t) ** 2
        k = 1
        while k < 5:
            if k != m and myres[k] != 0:
                var = var + (myres[k] - t) ** 2
            k = k + 1
        var = var / 4
        sql1 = 'UPDATE ' + CIname + ' SET reachable_proportion_T' + str(m) + '=' + str(p) + ',reachable_average=' + \
               str(t) + ',reachable_variance=' + str(var) + ' WHERE id=' + str(i) + ';'
        print(sql1)
        cursor.execute(sql1)
        db.commit()
    db.close()


def fromupdates(collectionname, CIname):
    # CIname = "CI2022" + collectionname[12:14] + "_0" + giveanum(collectionname)
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["routeviews_UPDATEs"]
    mycol = mydb[collectionname]
    host1 = ''
    user1 = ''
    password1 = ''
    dbname = 'Control_index'
    db = pymysql.connect(host=host1,
                         user=user1,
                         password=password1,
                         database=dbname)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    i = 0
    while True:
        i = i + 1
        sql3 = "SELECT prefix,update_times,live_time,last_time,last_id,repeat_announce,redundancy_cancel FROM " + \
               CIname + " WHERE id=" + str(i) + "; "
        cursor.execute(sql3)
        myres = cursor.fetchone()
        if myres is None:
            break
        updt = myres[1]
        lvt = myres[2]
        lt = myres[3]
        lid = myres[4]
        lrct = ''
        rpa = myres[5]
        rddc = myres[6]
        lst = 'W'
        myquery = {"PREFIX": myres[0]}
        print(myquery)
        mgres = mycol.find(myquery)
        if mgres is None:
            continue
        if lt is not None:
            mgcol = mydb[lt]
            myquery = {"_id": ObjectId(lid)}
            mgres1 = mgcol.find_one(myquery)
            lrct = mgres1["TIME"]
        for j in mgres:
            print(j)
            updt = updt + 1
            if lst == 'A':
                lvt = lvt + computetime(j["TIME"], lrct)
            if j["TARGET_NETWORK"]["INFO_TYPE"] == 'A':
                if lt is not None:
                    if mgres1["TARGET_NETWORK"]["START_AS"] == j["TARGET_NETWORK"]["START_AS"] and \
                            mgres1["ATTRIBUTE"]["AS_PATH"] == j["ATTRIBUTE"]["AS_PATH"]:
                        rpa = rpa + 1
            else:
                rddc = rddc + 1
            mgres1 = j
            lt = collectionname
            lid = j["_id"]
            lrct = j["TIME"]
            lst = j["TARGET_NETWORK"]["INFO_TYPE"]
        if lt is not None:
            sql1 = 'UPDATE ' + CIname + ' SET last_time="' + lt + '",last_id="' + str(lid) + '",update_times=' + str(updt) + \
                   ',live_time=' + str(lvt) + ',repeat_announce=' + str(rpa) + ',redundancy_cancel=' + str(rddc) + \
                   ' WHERE id=' + str(i) + ';'
        else:
            sql1 = 'UPDATE ' + CIname + ' SET update_times=' + str(updt) + \
                   ',live_time=' + str(lvt) + ',repeat_announce=' + str(rpa) + ',redundancy_cancel=' + str(rddc) + \
                   ' WHERE id=' + str(i) + ';'
        print(sql1)
        cursor.execute(sql1)
        db.commit()
    db.close()


if __name__ == "__main__":
    # init("CI202203_02")
    # fromupdates函数即 第一项为routeviews_UPDATES库中的集合名称，第二项为mysql中更新的Control_index表单名称，可获得除reachable外的所有属性
    # fromupdates("updates_20220316_1445", "CI202203_01")
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["routeviews_UPDATEs"]
    j0 = mydb.list_collection_names()
    print(j0[0])
    # init("CI202202_02t")
    # woshishabi
    time1 = time.time()
    fromupdates(j0[1], "CI202202_02t")
    time2 = time.time()
    print("cost ", time2 - time1, "s")
    # fromPing函数即 第一项为Ping库中的集合名称，第二项为mysql中更新的Control_index表单名称，可获得reachable的所有属性
    # fromPing("PD20220322_04", "CI202203_03")
    # trace_consis一直无表单，因此还没实现
    # 还未整体跑过一遍，因为对于proportion的T1,T2,T3,T4界定不是很明确，目前设置为早上6点前的为T1,以此类推
    # 函数均可正常工作，已用100个样本的CI202203_03表单测试，即CI202203_03中只有100个目标前缀
