from pymongo import MongoClient
import re
import os
import time


# 功能:生成一个新的字典地址，以免相同变量的变化导致字典元素前后重复
def new_dic(mydic):
    dic = {'TIME': mydic['TIME'],
           'TYPE': mydic['TYPE'],
           'SEND_NEIGHBOR': mydic['SEND_NEIGHBOR'],
           'LOCAL_BGP': mydic['LOCAL_BGP'],
           'ATTRIBUTE': mydic['ATTRIBUTE'],
           'TARGET_NETWORK':
               {
                   'START_AS': mydic['TARGET_NETWORK']['START_AS'],
                   'INFO_TYPE': mydic['TARGET_NETWORK']['INFO_TYPE'],
                   'PREFIX': mydic['TARGET_NETWORK']['PREFIX']
               },
           'PREFIX': mydic['PREFIX']
           }
    return dic


# 功能:对解析完毕后的updates包进行标准格式化，所传参数为解析后的txt文本的实际路径
def format(f_path):
    print('start processing ', f_path[7:])
    # Gets all target prefixes
    aim_pre = {}
    # 读取目标前缀，用于后续的筛选
    pre_txts = os.listdir('./aim_prefix')
    for pre_txt in pre_txts:
        pre_path = './aim_prefix/' + pre_txt
        f = open(pre_path, 'r')
        aim_bgps = f.read()
        f.close()
        aim_bgp = aim_bgps.split('\n')
        num = pre_txt.split('.')[0]
        aim_pre[num] = aim_bgp

    f = open(f_path, 'r')
    str = f.read()
    all = str.split('\n\n')
    f.close()

    all_dic = []
    for update in all:
        if 'TIME' in update:
            # 创建一个空字典用于存储所需信息
            dic = {'TIME': '',
                   'TYPE': {},
                   'SEND_NEIGHBOR': {},
                   'LOCAL_BGP': {},
                   'ATTRIBUTE': {},
                   'TARGET_NETWORK': {},
                   'PREFIX': ''
                   }
            dict_type = {'MESSAGE_TYPE': '',
                         'TIME': ''
                         }
            dict_sneigh = {'IP': '',
                           'ASN': ''
                           }
            dict_locBGP = {'IP': '',
                           'ASN': ''
                           }
            dict_attr = {'ORIGIN': '',
                         'AS_PATH': [],
                         'ATOMIC_AGGREGATE': '',
                         'AGGREGATOR': {},
                         'UNKNOWN_ATTR': [],
                         'COMMUNITY': {},
                         'LOCAL_PREF': '',
                         'LARGE_COMMUNITY': '',
                         'MED': ''
                         }
            dict_target = {'START_AS': '',
                           'INFO_TYPE': '',
                           'PREFIX': ''
                           }
            list_attr_apath = []
            dict_attr_aggtor = {'ASN': '',
                                'IP': ''
                                }
            list_attr_uattr = []
            dict_attr_comu = {}

            # 报文更新时间
            dic['TIME'] = re.findall(r'TIME: (.+)\n?', update)[0]
            # print(dic['TIME'])

            # 报文类型:update类型的表示为15min周期
            dict_type['MESSAGE_TYPE'] = re.findall(r'TYPE: (.+)\n?', update)[0]
            dict_type['TIME'] = '15MIN'
            dic['TYPE'] = dict_type
            # print(dic['TYPE'])

            # 前一跳邻居对等体所属的AS以及BGPPeer的IP地址
            dict_sneigh['IP'] = re.findall(r'FROM: (.+?) ', update)[0]
            dict_sneigh['ASN'] = re.findall(r'FROM:.+(AS.+)', update)[0]
            dic['SEND_NEIGHBOR'] = dict_sneigh
            # print(dic['SEND_NEIGHBOR'])

            # 接收该报文的本地AS以及BGPPeer的IP地址
            dict_locBGP['IP'] = re.findall(r'TO: (.+?) ', update)[0]
            dict_locBGP['ASN'] = re.findall(r'TO:.+(AS.+)', update)[0]
            dic['LOCAL_BGP'] = dict_locBGP
            # print(dic['LOCAL_BGP'])

            # 该目的前缀的始发类型，包括IGP，EGP，INCOMPLETE
            if 'ORIGIN' in update:
                dict_attr['ORIGIN'] = re.findall(r'ORIGIN: (.+?)\n', update)[0]
            # 到达目的前缀所经过的AS路径，出发顺序是从左至右
            if 'ASPATH' in update:
                attr_apath = re.findall(r'ASPATH: (.+?)\n', update)[0]
                list_attr_apath = attr_apath.split(' ')
                dict_attr['AS_PATH'] = list_attr_apath
            # 是否进行了路由自动聚合操作，没有自动聚合则为False
            if 'ATOMIC_AGGREGATE' in update:
                dict_attr['ATOMIC_AGGREGATE'] = 'True'
            else:
                dict_attr['ATOMIC_AGGREGATE'] = 'False'
            # 实施自动聚合的BGPPeer所在AS以及IP地址，没有则为空
            if 'AGGREGATOR' in update:
                dict_attr_aggtor['ASN'] = re.findall(r'AGGREGATOR: (.+?) \d?', update)[0]
                dict_attr_aggtor['IP'] = re.findall(r'AS\d+ (.+?)\n', update)[0]
                dict_attr['AGGREGATOR'] = dict_attr_aggtor
            # 未知属性，没有则为空
            if 'UNKNOWN' in update:
                list_attr_uattr.append(re.findall(r'UNKNOWN_ATTR(.+?):', update)[0])
                list_attr_uattr.append(re.findall(r'[)]: (.+?)\n?', update)[0])
                dict_attr['UNKNOWN_ATTR'] = list_attr_uattr
            # BGP的团体属性，键表示AS号，值表示所属团体号，没有则为空
            if 'COMMUNITY' in update:
                coms = re.findall(r'COMMUNITY: (.+?)\n', update)[0]
                if ':' in coms:
                    coms = coms.split(' ')
                    for com in coms:
                        if ':' in com:
                            s_com = com.split(':')
                            if s_com[0] in dict_attr_comu:
                                dict_attr_comu[s_com[0]].append(s_com[1])
                            else:
                                dict_attr_comu[s_com[0]] = []
                                dict_attr_comu[s_com[0]].append(s_com[1])
                        else:
                            if com not in dict_attr_comu:
                                dict_attr_comu[com] = []
                else:
                    dict_attr_comu[coms] = []
                dict_attr['COMMUNITY'] = dict_attr_comu
            # BGP的本地优先属性，没有则为空
            if 'LOCAL' in update:
                dict_attr['LOCAL_PREF'] = re.findall(r'LOCAL_PREF: (.+?)\n', update)[0]
            # 新出现的一个属性，意义未知
            if 'LARGE_COMMUNITY' in update:
                dict_attr['LARGE_COMMUNITY'] = re.findall(r'LARGE_COMMUNITY: (.+?)\n', update)[0]
            # BGP的MULTI_EXIT_DISC属性，没有则为空
            if 'MULTI_EXIT_DISC' in update:
                dict_attr['MED'] = re.findall(r'MULTI_EXIT_DISC: (.+?)\n', update)[0]
            dic['ATTRIBUTE'] = dict_attr

            # 目标前缀所在的AS号
            if 'ASPATH' in update:
                dict_target['START_AS'] = list_attr_apath[-1]

            # 该UPDATE消息是用来进行路由宣告还是路由撤销的，宣告值为A，撤销值为W
            if 'ANNOUNCE' in update:
                dict_target['INFO_TYPE'] = 'A'
                announce = re.findall(r'ANNOUNCE\s+([\s\S]*)', update)[0]
                announce = announce.replace(' ', '')
                list_ann = announce.split('\n')
                for pref in list_ann:
                    if pref != '\n' and pref != '':
                        head = pref.split('.')[0]  # Gets the prefix first bit
                        if head in aim_pre:
                            if pref in aim_pre[head]:
                                dict_target['PREFIX'] = pref
                                dic['PREFIX'] = pref
                                dic['TARGET_NETWORK'] = dict_target
                                dic_new = new_dic(dic)
                                all_dic.append(dic_new)
            if 'WITHDRAW' in update:
                dict_target['INFO_TYPE'] = 'W'
                if 'ANNOUNCE' in update:
                    withdraw = re.findall(r'WITHDRAW\s+([\s\S]*)A', update)[0]
                else:
                    withdraw = re.findall(r'WITHDRAW\s+([\s\S]*)', update)[0]
                withdraw = withdraw.replace(' ', '')
                list_wit = withdraw.split('\n')
                for pref in list_wit:
                    if pref != '\n' and pref != '':
                        head = pref.split('.')[0]  # Gets the prefix first bit
                        if head in aim_pre:
                            if pref in aim_pre[head]:
                                dict_target['PREFIX'] = pref
                                dic['PREFIX'] = pref
                                dic['TARGET_NETWORK'] = dict_target
                                dic_new = new_dic(dic)
                                all_dic.append(dic_new)
    print(f_path[7:], 'has been formated.')
    return all_dic


# 功能:把标准化后的数据上传到服务器的数据库中，参数all_dic即所传的字典数据，table为集合名
def c_mongodb(all_dic, table):
    print('start transport ', table)
    client = MongoClient('', )    # 连接到''的mongodb
    db = client['routeviews_UPDATEs']   # 选择数据库'routeviews_UPDATEs'
    collection = db[table]      # 选择或创建集合table
    try:
        collection.insert_many(all_dic)     # 插入数据
        client.close()
        print(table, 'inserts successfully!')
        return 1
    except:
        print(table, ' inserts failed.')
        # 若存储失败，将此错误记录到文件中
        f = open('warning.txt', 'a')
        f.write(table + ' inserts to mongodb failed!' + '\n')
        f.close()
        return 0


# 功能:调用格式化和存储函数，处理解析后的txt文件，参数file为所需处理的文件名，具体路径在函数中修改，s_path为对应的update包的具体路径
def format_tmongo(file, s_path):
    r = 0
    if '.txt' in file:
        flag = 1    # 标记，表示格式标准化是否完成
        f_path = './txts/' + file   # 生成所需格式化txt文件的路径，根据''实际存储位置''进行修改
        table = 'updates_' + file[:8] + '_' + file[8:-4]    # 生成集合名，根据实验室项目需求设定
        start_time1 = time.time()
        try:
            d_dict = format(f_path)
        except:
            print(file, 'formating failed!')
            # 格式化失败，将此错误写入warning.txt文件中
            f = open('warning.txt', 'a')
            f.write(file + ' formats failed!' + '\n')
            f.close()
            # 格式化失败，将此文件写入un_pro.txt文件中，交给另一个程序来处理
            f = open('un_pro.txt', 'a')
            f.write(s_path[26:] + '\n')
            f.close()
            # 格式化失败，表示本程序无法处理，所以对本程序标记为处理成功，即跳过本程序
            f = open('bgp_analyse.txt', 'a')
            f.write(s_path[26:] + '\n')
            f.close()
            os.remove(s_path)   # 格式化失败，有可能是因为update包下载不完整，删除update包，让下载任务重新下载
            flag = 0
        end_time1 = time.time()
        time1_consume = end_time1 - start_time1
        print('formats time:', time1_consume)

        # 若格式化失败，则不必继续存储模块
        if flag:
            start_time2 = time.time()
            if c_mongodb(d_dict, table):
                print(file, 'has been processed.')
                r = 1
            else:
                os.remove(s_path)   # 存储失败，有可能是因为update包下载不完整，删除update包，让下载任务重新下载
            os.remove(f_path)   # 删除解析后的txt文件，减少存储空间占用
            end_time2 = time.time()
            time2_consume = end_time2 - start_time2
            print('time insert to mongodb:', time2_consume)
    return r


# 功能:解析updates包，s_path为update包的位置，a_path为解析后文件的位置
def analyse(s_path, a_path):
    cmd = 'bgpdump -H ' + s_path + ' > ' + a_path
    os.system(cmd)


# 功能:遍历本月updates包的文件夹，将未处理的包传给format_tmongo()处理
def to_deal():
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    b_dir = './updates/updates_month' + t[5:7]
    files = os.listdir(b_dir)   # 本月已下载的updates包

    # 读取处理完毕的文件，避免重复处理
    f = open('./bgp_analyse.txt', 'r')
    p_updates = f.read()
    p_update = p_updates.split('\n')
    f.close()
    files.sort()    # 排序，后面可以按照时间顺序对文件进行解析处理
    for file in files:
        if file not in p_update:    # 处理过的文件跳过
            print('start processing ', file)
            time1 = time.time()
            s_path = b_dir + '/' + file     # 生成update包的路径
            a_path = './txts/' + file[8:16] + file[17:21] + '.txt'  # 生成解析后txt文件的路径
            analyse(s_path, a_path)
            txt_file = file[8:16] + file[17:21] + '.txt'
            if format_tmongo(txt_file, s_path):
                # 若处理无误，将此update包记录到txt文件中
                f = open('./bgp_analyse.txt', 'a')
                f.write(file + '\n')
                f.close()
                p_update.append(file)
            time2 = time.time()
            print(file, ' costs: ', time2 - time1)
        else:
            print(file, 'has been processed or is processed failed.')

    end_time = time.ctime()
    print(end_time)


def main():
    # 循环调用，每15分钟调用一次程序，实现自动化
    while 1:
        start_time = time.time()
        to_deal()
        end_time = time.time()
        a_time = end_time - start_time
        if a_time < 900:
            time.sleep(900 - a_time)


if __name__ == '__main__':
    main()
