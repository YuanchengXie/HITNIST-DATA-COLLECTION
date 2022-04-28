'''
该脚本把dataplane_path文件上传到文件服务器,
并在探测服务器上开启相关的探测服务, 保存任务id,
然后进入死循环等待探测任务的结束, 探测任务结束后,
从文件服务器下载探测结果文件到本地, 并进行相关的处理,
最终结果存进数据库
'''

from itertools import cycle
import ping_tool
import time
import random
import pymongo
import json
import datetime
import os
import oss2
import zipfile

n=1 #并发量, 由于服务器特性, 不需要更改
dataplane_path = 'dataplane_5k.txt' #待探测的前缀文件路径
#dataplane_path2 = 'dataplane_5k.txt'

input_file_paths = []
localtime = time.localtime()
localtime_str = time.strftime('%Y%m%d%H%M%S', localtime)
uploadfile_names = []
task_ids = []
outputfile_names = []
logfile_path = ''
final_outfilename = ''
sleep_time = 600
cycle_minute = 360

def split_file(dataplane_path, n):
    '''
    根据并发量对目标前缀文件进行分割
    '''
    file_pool = []
    file_paths = []
    for i in range(n):
        file_pool.append(open('./temp/temp_dataplane'+str(i),'w+'))
        file_paths.append('./temp/temp_dataplane'+str(i))
    f = open(dataplane_path,'r')
    while True:
        line = f.readline()
        if not line:
            break
        file_pool[random.randint(0,n-1)].write(line)
    for i in range(n):
        file_pool[i].close()
    f.close()
    #print('split file complete!\n')
    return file_paths

def log(string):
    global logfile_path
    with open(logfile_path, 'a', encoding='utf-8') as flog:
        flog.write(str(string))

def process_data(pre_proce_file_path):
    log("start build a generator for data processing!\n")
    with open(pre_proce_file_path, 'r', encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break
            outputline = line[:len(line)-1]
            yield outputline

def save2db(pre_proce_file_path,localtime):
    '''
    将数据保存到数据库中
    '''
    host = ""
    port = ""
    db_name = "Ping"
    col_name = "PD" + time.strftime("%Y%m%d_%H", localtime)
    myclient = pymongo.MongoClient("mongodb://" + host + ":" + port + "/",connect=True)
    db = myclient[db_name]
    col = db[col_name]
    count = 0
    #data = process_data(output_file_path) #build the generator
    start_time = time.perf_counter()
    data = process_data(pre_proce_file_path) #build the generator
    docs = []
    while True:
        try:
            docs.append(next(data))
        except StopIteration:
            break
    col.insert_one({"IP":docs})
    count = len(docs)
    log("saved %d times!\n"%count)
    end_time = time.perf_counter()
    log("Running time: %s Second\n"%(end_time-start_time))
    log("saving data completed!!!\n")
    myclient.close()

def save(pre_proce_file_path, localtime):
    uploadfile_name = 'PD'+time.strftime("%Y%m%d_%H", localtime) + '.json.zip'
    processed_file_path = './temp/' + 'PD' + \
         time.strftime("%Y%m%d_%H", localtime) + '.json'
    count = 0
    start_time = time.perf_counter()
    data = process_data(pre_proce_file_path) #build the generator
    docs = []
    while True:
        try:
            docs.append(next(data))
        except StopIteration:
            break
    with open(processed_file_path, 'w+', encoding='utf-8') as fout:
        json.dump({"IP":docs}, fout)
    zipFile = zipfile.ZipFile(processed_file_path+'.zip', 'w')
    zipFile.write(processed_file_path, processed_file_path[7:], zipfile.ZIP_DEFLATED)
    zipFile.close()
    auth = oss2.Auth('', '')
    bucket = oss2.Bucket(auth, '',\
         '')
    bucket.put_object_from_file('ping/data/'+uploadfile_name, processed_file_path+'.zip')
    log("saved %d times!\n"%count)
    end_time = time.perf_counter()
    log("Running time: %s Second\n"%(end_time-start_time))
    log("saving data completed!!!\n")


def get_last_time_started(logfilepath):
    '''
    根据总日志文件logfilepath获取上一次开启任务的时间
    '''
    try:
        flog = open(logfilepath, 'r', encoding='utf-8')
        content = flog.readlines()
        flog.close()
    except FileNotFoundError: #如果没有相应的总日志文件, 则返回默认时间:2022/01/01/00:00
        time_log = datetime.datetime.strptime('202201010000','%Y%m%d%H%M')
        return time_log
    if len(content) == 0: #如果总日志文件里面没有内容, 则说明该次启动为第一次, 返回默认时间:2022/01/01/00:00
        time_log = datetime.datetime.strptime('202201010000','%Y%m%d%H%M')
        return time_log
    else :
        for i in content:
            if i.count('start') > 0: #根据start标识, 判断该行是否为启动任务时记录的日志
                time_log = datetime.datetime.strptime(i[0:12],'%Y%m%d%H%M')
        return time_log

if __name__ == '__main__':
    print('master pid is %s' % os.getpid()) #输出主进程id
    while True:
        time_log = get_last_time_started('task1_log')
        t = datetime.datetime.strptime(time.strftime('%Y%m%d%H%M',time.localtime()), '%Y%m%d%H%M')
        flog = open('task1_log', 'a+', encoding='utf-8')
        if (t-time_log).seconds/60 >= cycle_minute:
            start = time.perf_counter()
            input_file_paths = split_file(dataplane_path,n)
            uploadfile_names = []
            localtime = time.localtime()
            localtime_str=time.strftime('%Y%m%d%H%M%S', localtime)
            task_ids = []
            outputfile_names = []
            logfile_path = './log/log-ping-'+localtime_str
            final_outfilename = 'ping-'+localtime_str+'.txt'
            flog.write('%s start a task1 test\n'%time.strftime('%Y%m%d%H%M',time.localtime()))
            for i in range(n): #生成分割前缀文件路径
                uploadfile_names.append('upload-'+str(i)+'.txt')
            for i in range(n):#上传文件
                log(str(i)+':')
                log(str(ping_tool.upload(input_file_paths[i],uploadfile_names[i]))+'\n')
            for i in range(n): #创建任务并且获取相应的任务id
                task_ids.append(json.loads(ping_tool.creat_task(uploadfile_names[i])))
            for i in task_ids: #使用任务ip获取结果文件名称
                outputfile_names.append(json.loads(ping_tool.taskid(i["task_id"])))
            flog.write('%s creat tasks complete!\n'%time.strftime('%Y%m%d%H%M',time.localtime()))
            while True:
                flag = 1
                count = n
                for i in task_ids:
                    t = json.loads(ping_tool.taskid(i["task_id"]))
                    if t["target"]["status"] != "done":
                        flag = 0
                        count -= 1
                if flag == 1:
                    break
                flog.write('%s Waiting...%d/%d\n'%(time.strftime('%Y%m%d%H%M',time.localtime()),count,n))
                time.sleep(10)
            fout = open(final_outfilename, 'w+', encoding='utf-8')
            for i in outputfile_names:
                t = ping_tool.getresult(i["target"]["outputFilename"])
                fout.write(t)
            fout.close()
            #save2db(final_outfilename,localtime) #save the data to the mongodb
            save(final_outfilename,localtime)
            end = time.perf_counter()
            flog.write('%s end a task1, runtime %s seconds\n'%\
                (time.strftime('%Y%m%d%H%M',time.localtime()),(end-start)))
        else:
            flog.write('test: now %s log %s \n'%(str(t),str(time_log)))
            flog.write('test: passed %s seconds\n'%str((t-time_log).seconds))
            flog.write('%s waiting...\n'%time.strftime('%Y%m%d%H%M',time.localtime()))
        flog.close()
        time.sleep(sleep_time)
