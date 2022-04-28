# -*- coding:utf-8 -*-

import sys
import awdb
import random
import time


def get_random_ip():
    return ".".join([str(x) for x in [random.randrange(0,255),
                     random.randrange(0,255),
                     random.randrange(0,255),
                     random.randrange(0,255)]])

def la(ip):
    #IP_trial_single_WGS84.awdb  文件存放位置
    filename = r'C:\Users\sun\Desktop\匿名节点\IP_trial_single_WGS84.awdb'
    reader = awdb.open_database(filename)
    (record, prefix_len) = reader.get_with_prefix_len(ip)
    if "." in ip:
        continent = record.get("continent", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("continent", '')
        country = record.get("country", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("country", '')
        zipcode = record.get("zipcode", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("zipcode", '')
        timezone = record.get("timezone", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("timezone",'')
        accuracy = record.get("accuracy", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("accuracy",'')
        source = record.get("source", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("source", '')
        owner = record.get("owner", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("owner", '')
        lngwgs = record.get("lngwgs", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("lngwgs", '')


        multiAreas = record.get("multiAreas", {})
        if multiAreas:
            for area in multiAreas:
                prov = area.get("prov", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("prov", "")
                city = area.get("city", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("city", "")
                district = area.get("district", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get(
                    "district", "")
                latwgs = area.get("latwgs", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("latwgs", "")
                lngwgs = area.get("lngwgs", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("lngwgs", "")
                latbd = area.get("latbd", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("latbd", "")
                lngbd = area.get("lngbd", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("lngbd", "")
                radius = area.get("radius", b"").decode("utf-8") if sys.version_info[0] == 3 else area.get("radius", "")

    
    elif ":" in ip:
        province = record.get("province", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("province", '')
        city = record.get("city", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("city", '')
        source = record.get("source", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("source", '')
        areacode = record.get("areacode", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("areacode",'')
        country = record.get("country", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("country", '')
        latwgs = record.get("latwgs", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("latwgs", '')
        isp = record.get("isp", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("isp", '')
        lngwgs = record.get("lngwgs", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("lngwgs", '')
        zipcode = record.get("zipcode", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("zipcode", '')
        owner = record.get("owner", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("owner", '')
        asnumber = record.get("asnumber", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("asnumber", '')
        latbd = record.get("latbd", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("latbd", '')
        timezone = record.get("timezone", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("timezone",'')
        lngbd = record.get("lngbd", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("lngbd", '')
        continent = record.get("continent", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("continent", '')
        accuracy = record.get("accuracy", b'').decode("utf-8") if sys.version_info[0] == 3 else record.get("accuracy",'')

    else:
        print ("!")
        

    return record['latwgs'].decode('utf-8'),record['lngwgs'].decode('utf-8')#返回经纬度

