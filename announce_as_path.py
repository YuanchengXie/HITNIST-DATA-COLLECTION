from pymongo import MongoClient
import time


# 功能:读取指定集合的update数据，记录宣告aspath
def grep_as(collection):
    s_time = time.time()
    print('start reading collection:', collection)
    try:
        client = MongoClient('mongodb://')  # 连接到''的mongodb
        db = client['routeviews_UPDATEs']  # 选择数据库routebiews_UPDATEs
        col = db[collection]  # 选择集合collection
        datas = col.find({})    # 获取集合中的所有数据
    except:
        print('connect to mongo failed!!!')
        return
    # 遍历所有数据记录对应目标前缀的宣告aspath
    for data in datas:
        TDC = {'prefix': data['PREFIX'], 'trace_AS_path': [], 'announce_AS_path': data['ATTRIBUTE']['AS_PATH']}
        all_prefix[data['PREFIX']] = TDC
    # result = []
    # for value in all_prefix.values():
    #     result.append(value)
    e_time = time.time()
    print(collection, ' takes %ds' % (e_time - s_time))


# 功能:将一整天的目标前缀的aspath记录完毕后存储到数据库中，table为目标集合名，list为需要存储的数据
def c_mongodb(table, list):
    print('star storing %s!!' % (table))
    try:
        client = MongoClient('', )
        db = client['Trace_consis']
        collection = db[table]
        collection.insert_many(list)
        return 1
    except:
        print('%s interts failed!!' % (table))
        return 0


# 功能:对某年某日的所有updates数据进行读取记录，year为年，date为月日(月日格式为四位字符串，单位数日期需要提前补0)
def select_as(year, date):
    flag = 0    # 标志，记录该日期的aspath路径读取是否完毕
    s_time = time.time()
    today = date
    global all_prefix   # 全局变量，存储当天所有前缀的aspath信息
    all_prefix = {}
    try:
        client = MongoClient('mongodb://')
        db = client['routeviews_UPDATEs']
        collections = db.list_collection_names()
    except:
        print('connect to mongo failed!!!')
        return
    aims = []
    # 抽取当天的updates包
    for collection in collections:
        if 'updates' in collection:
            if collection[12:16] == today:
                aims.append(collection)
    aims.sort()     # 排序，按时间读取，确保as_path为最新的update包中读取出来的
    # 一天的updates包理论上有96个，若不足96个，则表明数据尚未处理完毕，跳过
    if len(aims) > 95:
        for aim in aims:
            grep_as(aim)
        pre_list = []
        for value in all_prefix.values():
            pre_list.append(value)
        table = 'TDC_' + year + date
        print(len(aims))
        e_time = time.time()
        print('total time:%ds' % (e_time - s_time))
        flag = c_mongodb(table, pre_list)
    else:
        print(year, date, 'wait for updates complete.')
    return flag


# 功能:生成一个月所有的日期，用户后续读取处理updates包
def y_month(year, month):
    date = month * 100 + 1
    mon = []
    if month in [1, 3, 5, 7, 8, 10, 12]:
        days = 31
    elif month in [4, 6, 9, 11]:
        days = 30
    else:
        if year % 400 == 0 or (year % 4 == 0 and year % 100 != 0):
            days = 29
        else:
            days = 28
    if month < 10:
        for i in range(0, days):
            md = '0' + str(date + i)
            mon.append(md)
    else:
        for i in range(0, days):
            md = '0' + str(date + i)
            mon.append(md)
    return mon


# 功能:调用前面的函数，检查并处理本月需要读取的aspath
def annas():
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    year = t[0:4]
    month = t[5:7]
    aim_month = y_month(int(year), int(month))  # 本月的所有日期，格式为四位字符串
    # 查看并获取已经完成存储aspath的集合
    try:
        client = MongoClient('',)  # 连接到''的mongodb
        db = client['Trace_consis']  # 选择数据库Trace_consis
        collections = db.list_collection_names()
    except:
        print('connect to mongo failed!!!')
        return

    for date in aim_month:
        table = 'TDC_' + year + date    # 生成要存储到数据库中的集合名
        if table not in collections:    # 若collections中已存在此集合，跳过
            if select_as(year, date):
                collections.append(table)
            else:
                print(table, 'processes failed.')
        else:
            print(table, 'exists!!!')


def main():
    # 循环调用，每15分钟调用一次程序，实现自动化
    while 1:
        s_time = time.time()
        annas()
        e_time = time.time()
        a_time = e_time - s_time
        if a_time < 86400:
            time.sleep(86400 - a_time)


if __name__ == '__main__':
    main()
