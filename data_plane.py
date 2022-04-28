import pymongo
from bson.objectid import ObjectId


def testconnection():
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["Traceroute"]
    l0 = mydb.list_collection_names()
    l01 = []
    for i in l0:
        if '0316' in i:
            iid = i[-2:]
            l01.append({'id': iid, 'name': i})
    l01.sort(key=lambda x1: x1['id'])
    print(l01)
    del l01[2]
    for x in l01:
        print(x['name'])
        mycol = mydb[x['name']]
        print(x)
        y = mycol.find_one()
        print(y)
        print(y['_id'])


def fromTracert(collectionname, DIname):
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["Traceroute"]
    mycol = mydb[collectionname]
    mydb1 = myclient["Data_index"]
    mycol1 = mydb1[DIname]
    if mycol.find_one() is None:
        return
    x = mycol.find_one()["_id"]
    i = 0
    cnt0 = 0
    while True:
        data = {
            "time": "",
            "IP": "",
            "path_length": {
                "average": 0,
                "fluctuation": 0
            },
            "reachable": {
                "average_time": 0
            }
        }
        num = int(str(x), 16) + i
        idnum = str(hex(num))[2:]
        myquery = {"_id": ObjectId(idnum)}
        res = mycol.find_one(myquery)
        if res is None:
            break
        if len(res['results']) == 0:
            i = i + 1
            continue
        data["time"] = DIname[2:]
        data["IP"] = res['IP']
        cnt3 = 0
        time1 = 0
        myquery1 = {"IP": data["IP"]}
        tmp = {}
        tmp2 = {}
        x1 = mycol1.find_one(myquery1)
        if x1 is not None:
            tmp = dict.copy(x1['path_length'])
            tmp2 = dict.copy(x1)
            print(tmp)
            del tmp2['path_length']
            del tmp['average']
        for t in res['results']:
            if cnt3 == 0:
                while True:
                    cnt3 = cnt3 + 1
                    if (x1 is None) or 'T' + str(cnt3) not in x1["path_length"]:
                        data["path_length"]['T' + str(cnt3)] = 0
                        break
            if (t['ttl'] + 1)>data["path_length"]['T' + str(cnt3)]:
                data["path_length"]['T' + str(cnt3)] = t['ttl'] + 1
                time1 = float(t['rtt'])
        if x1 is not None:
            avep = float('%.2f' % (
                (data["path_length"]['T' + str(cnt3)] + x1["path_length"]["average"] * (cnt3 - 1)) / cnt3))
            avet = float('%.2f' % ((time1 + float(x1["reachable"]["average_time"]) * (cnt3 - 1)) / cnt3))
            tmp2["path_length"] = tmp
            tmp2["path_length"]["T" + str(cnt3)] = data["path_length"]['T' + str(cnt3)]
            tmp2["path_length"]["average"] = avep
            tmp2["reachable"]["average_time"] = avet
            if data["path_length"]['T' + str(cnt3)] != x1["path_length"]['T1']:
                print(tmp)
                nf = tmp["fluctuation"] + 1
                tmp2["path_length"]["fluctuation"] = nf
            print(tmp2)
            newvalues = {"$set": tmp2}
            res = mycol1.update_one(myquery1, newvalues)
            cnt0 = cnt0 + 1
        else:
            data["path_length"]["average"] = data["path_length"]['T' + str(cnt3)]
            data["reachable"]["average_time"] = time1
            data["path_length"]["fluctuation"] = 0
            res = mycol1.insert_one(data)
        i = i + 1
    print(cnt0)


def fromPing(DIname, collectionname):
    myclient = pymongo.MongoClient('mongodb://')
    mydb = myclient["Ping"]
    mycol = mydb[collectionname]
    x1 = mycol.find_one()["IP"]
    mydb1 = myclient["Data_index"]
    mycol1 = mydb1[DIname]
    if mycol1.find_one() is None:
        return
    x = mycol1.find_one()["_id"]
    i = 0
    ifin = 0
    flag = 0
    flag1 = 0
    while True:
        num = int(str(x), 16) + i
        idnum = str(hex(num))[2:]
        myquery = {"_id": ObjectId(idnum)}
        res = mycol1.find_one(myquery)
        if res is None:
            break
        tmp = dict.copy(res)
        if "change_times" in res["reachable"].keys():
            flag1 = 1
            if res["IP"] in x1:
                ifin = 1
            if ifin != res["reachable"]["whether"]:
                nt = res["reachable"]["change_times"] + 1
                nlrt = int(collectionname[-2:])
                flag = 1
            else:
                nlrt = res["reachable"]["lastrecord_time"]
                nt = res["reachable"]["change_times"]
            rectime = int(collectionname[-2:]) - res["reachable"]["lastrecord_time"]
        else:
            nlrt = int(collectionname[-2:])
            if res["IP"] in x1:
                ifin = 1
            nt = 0
            rectime = 0
        if nt == 0:
            if flag1 == 1:
                ncp = tmp["reachable"]["change_period"] + rectime
            else:
                ncp = 0
        else:
            if flag == 0:
                ncp = '%.2f' % ((tmp["reachable"]["change_period"]*nt + rectime)/nt)
            else:
                ncp = '%.2f' % ((tmp["reachable"]["change_period"]*(nt - 1) + rectime)/nt)
        tmp["reachable"]["whether"] = ifin
        tmp["reachable"]["change_times"] = nt
        tmp["reachable"]["lastrecord_time"] = nlrt
        tmp["reachable"]["change_period"] = ncp
        newvalues = {"$set": tmp}
        res = mycol1.update_one(myquery, newvalues)
        i = i + 1


# 以下这个main是基于已有多个数据集合而且产生的main
# 如果要做成自动化形式需修改
def main():
    myclient = pymongo.MongoClient('mongodb://')
    mydb1 = myclient["Traceroute"]
    mydb2 = myclient["Ping"]
    l0 = mydb1.list_collection_names()
    l01 = []
    l02 = []
    for j in ['0312', '0313', '0314', '0315', '0316', '0317']:
        for i in l0:
            if j in i:
                iid = i[-2:]
                l01.append({'id': iid, 'name': i})
        l01.sort(key=lambda x1: x1['id'])
        print(l01)
        for x in l01:
            print(x['name'])
            fromTracert(x['name'], "DI202203_02")
        l1 = mydb2.list_collection_names()
        for i in l1:
            if j in i:
                iid = i[-2:]
                l02.append({'id': iid, 'name': i})
        l02.sort(key=lambda x1: x1['id'])
        print(l02)
        for x in l02:
            print(x['name'])
            fromPing("DI202203_02", x['name'])


if __name__ == "__main__":
    # testconnection()
    main()
