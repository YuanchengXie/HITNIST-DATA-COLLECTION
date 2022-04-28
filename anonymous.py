from flask import Flask,jsonify,request
import requests
import json
import re
import laln

dic=re.compile('}{')

app = Flask(__name__)
@app.route('/anonymous',methods=['get'])

def anony():
    taskid = request.args.get('taskid')
    url='http://topomapping.oss-cn-zhangjiakou.aliyuncs.com/topores/'+taskid+'/raw.json'#原始数据
    response=requests.request("GET",url)
    f=dic.sub("}\n{",response.text).split('\n')
    ddict={}
    edges=[]#边
    tooltip={}#点(type：0为匿名节点 la,ln经纬度)
    nextip=[]#匿名节点的下一跳节点
    connectip={}#匿名节点的连接节点

    for m in range(len(f)):
        i=eval(f[m])
        ip=i['ip']
        results=i['rnode']
        rnode=i['rnode']#加入匿名节点后结果
        key=[int(x) for x in list(results.keys())]
        key.sort()
    
        #目的节点tooltip
        tooltip.setdefault(ip,{})['rtt']=i['rtt']
        tooltip.setdefault(ip,{})['type']='target'
        re=laln.la(ip)
        if re[0]!='':
            tooltip.setdefault(ip,{})['la']=re[0]
            tooltip.setdefault(ip,{})['ln']=re[1]
        else:
            tooltip.setdefault(ip,{})['la']='-'
            tooltip.setdefault(ip,{})['ln']='-'  
        
        for x in range(len(key)-1):
            #路由节点tooltip
            tooltip.setdefault(results[str(key[x])],{})['rtt']=i['rrtt'][str(key[x])]
            tooltip.setdefault(results[str(key[x])],{})['type']='route'
            re=laln.la(results[str(key[x])])
            if re[0]!='':
                tooltip.setdefault(results[str(key[x])],{})['la']=re[0]
                tooltip.setdefault(results[str(key[x])],{})['ln']=re[1]
            else:
                tooltip.setdefault(results[str(key[x])],{})['la']='-'
                tooltip.setdefault(results[str(key[x])],{})['ln']='-'  
                
            if key[x]+1!=key[x+1]:
                #匿名节点融合
                nextip.append(results[str(key[x+1])])
                index=nextip.index(results[str(key[x+1])])
                connectip.setdefault(index,[]).append(results[str(key[x+1])])
                connectip.setdefault(index,[]).append(results[str(key[x])])
                rnode[str(key[x]+1)]=index
                
                #匿名节点Edges:
                edges.append([results[str(key[x])],str(index),'0'])
                edges.append([str(index),results[str(key[x+1])],'0'])
            else:
                #路由节点Edges:
                edges.append([results[str(key[x])],results[str(key[x+1])],'1'])
                
        #最后一跳路由节点Edges:
        edges.append([results[str(len(key)-1)],ip,'1'])        
        tooltip.setdefault(results[str(len(key)-1)],{})['rtt']=i['rrtt'][str(len(key)-1)]
        tooltip.setdefault(results[str(len(key)-1)],{})['type']='route'
        re=laln.la(results[str(len(key)-1)])
        if re[0]!='':
            tooltip.setdefault(results[str(len(key)-1)],{})['la']=re[0]
            tooltip.setdefault(results[str(len(key)-1)],{})['ln']=re[1]
        else:
            tooltip.setdefault(results[str(len(key)-1)],{})['la']='-'
            tooltip.setdefault(results[str(len(key)-1)],{})['ln']='-'        
    
    #匿名节点定位
    for key in connectip:#遍历匿名节点的上下连接节点
        location=set(connectip[key])
        s=0
        la=0
        ln=0
        for i in location:#计算匿名节点地理位置（根据匿名节点的连接节点）
            re=laln.la(i)
            if re[0]!='':
                s=s+1
                if re[0][0]=='-':
                    la=la-float(re[0])
                else:
                    la=la+float(re[0])
                if re[1][0]=='-':
                    ln=ln-float(re[1])
                else:
                    ln=ln+float(re[1])
        if s==0:#上下节点经纬度未知，无法定位
            tooltip.setdefault(key,{})['la']='-'
            tooltip.setdefault(key,{})['ln']='-'
        else:
            la='{:.6f}'.format(la/s)
            ln='{:.6f}'.format(ln/s)
            tooltip.setdefault(key,{})['la']=str(la)
            tooltip.setdefault(key,{})['ln']=str(ln)
        tooltip.setdefault(key,{})['rtt']='0'
        tooltip.setdefault(key,{})['type']='Unknown'

    ddict['Edges']=edges
    ddict['tooltip']=tooltip
    return json.dumps(ddict)

app.run(host='127.0.0.1',port=8802,debug=True)
#http://127.0.0.1:8802/anonymous?taskid={taskid}