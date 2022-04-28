import requests
import time
import re
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# 功能:下载指定的updates包，若更换运行位置，注意文件存储路径的更改(l_path)
def down_task(bgp):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    url = 'http://archive.routeviews.org/bgpdata/' + t[0:4] + '.' + t[5:7] + '/UPDATES/'
    l_path = './updates/updates_month' + t[5:7]     # updates包存储文件夹
    # 扫描一遍文件夹中的文件，避免对已存在的updates包重复下载
    dbgps = os.listdir(l_path)
    if bgp in dbgps:
        print(bgp, 'exists!!')
        return
    bgp_url = url + bgp     # 目标url
    # request and store updates packages
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    proxies = {'http': ''}   # 代理，
    bgp_data = requests.get(url=bgp_url, headers=headers, proxies=proxies).content
    bgp_path = l_path + '/' + bgp   # updates包最终存储位置
    fsize = 0   # 标记，文件大小
    flag = 1    # 标记，本次下载任务是否完成
    print('start downloading', bgp)
    s_time = time.time()    # 记录开始下载时间
    while fsize < 2:    # 判断文件是否下载完毕，若小于2k认为未完成下载，重新下载此包
        with open(bgp_path, 'wb') as fp:
            fp.write(bgp_data)
            fp.close()
        fsize = int(os.path.getsize(bgp_path) / 1024)   # 读取文件大小并将其单位转化为kb
        n_time = time.time()
        if n_time - s_time > 600:   # 下载完一次后记录时间，若一个包的累积下载时间超过10分钟，跳过并记录此问题
            flag = 0
            break
    if flag:
        print(bgp, 'is stored!!')
    else:
        # 记录下载超时的包，并把此问题写入到文件中
        print(bgp, 'download too solw!!')
        f = open('warning.txt', 'a')
        f.write(bgp + " doesn't download successfully!" + "\n")
        f.close()
    print(bgp, 'size = %.1fM' % (fsize / 1024))


# 功能:返回前一个月的月份
def pre_mon(mon):
    mon = int(mon)
    if mon == 1:
        month = '12'
    elif mon < 11:
        month = '0' + str(mon - 1)
    else:
        month = str(mon - 1)
    return month


# 功能:根据系统时间确定需要下载的updates包，将包传到下载任务down_task()
def seek_aim():
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    # url = 'http://archive.routeviews.org/bgpdata/2022.03/UPDATES/'
    url = 'http://archive.routeviews.org/bgpdata/' + t[0:4] + '.' + t[5:7] + '/UPDATES/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    proxies = {'http': ''}
    start_time = time.time()
    page_text = requests.get(url=url, headers=headers, proxies=proxies).text
    bgps = re.findall(r'<a href="(.+?)">updates', page_text)    # 用正则表达式匹配出本月需要下载的updates包
    time1_time = time.time()
    # d_dgps = os.listdir('./test')
    # 用多线程进行下载，因为有可能一个时间放送出多个包，尽可能提高下载速度，线程数量可以根据实际情况更改
    with ProcessPoolExecutor(4) as p:
        for bgp in bgps:
            p.submit(down_task, bgp)
    end_time = time.time()
    print('the time of analyse html:', time1_time - start_time, 'total cost time:', end_time - start_time)


def main():
    # 循环调用，每15分钟调用一次程序，实现自动化
    while 1:
        s_time = time.time()
        seek_aim()
        e_time = time.time()
        a_time = e_time - s_time
        if e_time - s_time < 900:
            time.sleep(900 - a_time)


import requests
import time
import re
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# 功能:下载指定的updates包，若更换运行位置，注意文件存储路径的更改(l_path)
def down_task(bgp):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    url = 'http://archive.routeviews.org/bgpdata/' + t[0:4] + '.' + t[5:7] + '/UPDATES/'
    l_path = './updates/updates_month' + t[5:7]     # updates包存储文件夹
    # 扫描一遍文件夹中的文件，避免对已存在的updates包重复下载
    dbgps = os.listdir(l_path)
    if bgp in dbgps:
        print(bgp, 'exists!!')
        return
    bgp_url = url + bgp     # 目标url
    # request and store updates packages
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    proxies = {'http': 'http://r.hitwh.net.cn:40003'}   # 学长给的代理，放在实验室的虚拟机上用可以提高下载速度
    bgp_data = requests.get(url=bgp_url, headers=headers, proxies=proxies).content
    bgp_path = l_path + '/' + bgp   # updates包最终存储位置
    fsize = 0   # 标记，文件大小
    flag = 1    # 标记，本次下载任务是否完成
    print('start downloading', bgp)
    s_time = time.time()    # 记录开始下载时间
    while fsize < 2:    # 判断文件是否下载完毕，若小于2k认为未完成下载，重新下载此包
        with open(bgp_path, 'wb') as fp:
            fp.write(bgp_data)
            fp.close()
        fsize = int(os.path.getsize(bgp_path) / 1024)   # 读取文件大小并将其单位转化为kb
        n_time = time.time()
        if n_time - s_time > 600:   # 下载完一次后记录时间，若一个包的累积下载时间超过10分钟，跳过并记录此问题
            flag = 0
            break
    if flag:
        print(bgp, 'is stored!!')
    else:
        # 记录下载超时的包，并把此问题写入到文件中
        print(bgp, 'download too solw!!')
        f = open('warning.txt', 'a')
        f.write(bgp + " doesn't download successfully!" + "\n")
        f.close()
    print(bgp, 'size = %.1fM' % (fsize / 1024))


# 功能:返回前一个月的月份
def pre_mon(mon):
    mon = int(mon)
    if mon == 1:
        month = '12'
    elif mon < 11:
        month = '0' + str(mon - 1)
    else:
        month = str(mon - 1)
    return month


# 功能:根据系统时间确定需要下载的updates包，将包传到下载任务down_task()
def seek_aim():
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    # url = 'http://archive.routeviews.org/bgpdata/2022.03/UPDATES/'
    url = 'http://archive.routeviews.org/bgpdata/' + t[0:4] + '.' + t[5:7] + '/UPDATES/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    proxies = {'http': 'http://r.hitwh.net.cn:40003'}
    start_time = time.time()
    page_text = requests.get(url=url, headers=headers, proxies=proxies).text
    bgps = re.findall(r'<a href="(.+?)">updates', page_text)    # 用正则表达式匹配出本月需要下载的updates包
    time1_time = time.time()
    # d_dgps = os.listdir('./test')
    # 用多线程进行下载，因为有可能一个时间放送出多个包，尽可能提高下载速度，线程数量可以根据实际情况更改
    with ProcessPoolExecutor(4) as p:
        for bgp in bgps:
            p.submit(down_task, bgp)
    end_time = time.time()
    print('the time of analyse html:', time1_time - start_time, 'total cost time:', end_time - start_time)


def main():
    # 循环调用，每15分钟调用一次程序，实现自动化
    while 1:
        s_time = time.time()
        seek_aim()
        e_time = time.time()
        a_time = e_time - s_time
        if e_time - s_time < 900:
            time.sleep(900 - a_time)


if __name__ == '__main__':
    main()