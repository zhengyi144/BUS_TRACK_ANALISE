#coding=utf-8
from pymongo import MongoClient,ASCENDING
import numpy as np
import time
import os
from datetime import datetime,date,timedelta
from utils.getStations import getStationList
#from bson.code import Code
from utils.GPSConvertUtils import *

mon_ipAdrr="10.10.116.200"
mon_port=27017
mon_conn=MongoClient(mon_ipAdrr,mon_port)
mon_db=mon_conn.kctest

MIN_DIST=80      #meters
TIME_DELTA=1800  #seconds
DIST_DELTA=1000  #meters
DEPARTURE_TIME_OFFSET=20 #departure time offset(seconds)
DIST_OFFSET=500 #departure distance offset(meters)
LOOP_LINE_NO=['196']
LOOP_TIME_DELTA=300

def getMinIndexes(dists):
    """
    计算每个GPS点距离最近的站点
    """
    r,c=np.shape(dists)
    minIndexes=[]
    temp=[]
    for j in range(r):
        c_index=np.argmin(dists[j,:])
        temp.append(c_index)
    temp=np.asarray(temp)
    for i in range(c):
        r_index=np.where(temp==i)
        if len(r_index[0])>0:
            #print(dists[r_index,i])
            idx=np.argmin(dists[r_index,i])
            #print(r_index[0][idx])
            rIndex=r_index[0][idx]
            minIndexes.append({"minDist":dists[rIndex,i],"rIndex":r_index[0][idx],"cIndex":i})
        else:
            #如果没有坐标该站点附近，则取距离改站点最近的点为参考
            rIndex=np.argmin(dists[:,i])
            minIndexes.append({"minDist":dists[rIndex,i],"rIndex":rIndex,"cIndex":i})
    #按照站点顺序进行排序
    minIndexes.sort(key=lambda x:x["cIndex"])
    return minIndexes

def getTimeDelta(locs,minIndexes,stationList,busNo,label_time):
    """
    计算站点之间时间差
    input:
        locs:[{"lng","lat","site_time"},{},{}]
        minIndexes:[{"minDist","rIndex","cIndex"},{},{}]
        stationList:{"LINE_NO","IS_UP_DOWN","LOCS":[{"LABEL_NO","LNG","LAT"},{}]}
        "OD":"opposite direction"
        "LT":"last time over offset"
        "NT":"next time over offset"
        ""
    output:
        return timeDeltas:[{"6_0_1_2":seconds,"last_station_time","next_station_time"},{},{}]
    """
    try:
        stationNum=len(minIndexes)
        locsNum=len(locs)
        lineName=stationList["LINE_NO"]+"_"+str(stationList["IS_UP_DOWN"])
        #计算时间差
        timeDeltas=[]
        for i in range(stationNum-1):
            last_minDist=minIndexes[i]["minDist"]
            last_index=minIndexes[i]["rIndex"]
            next_minDist=minIndexes[i+1]["minDist"]
            next_index=minIndexes[i+1]["rIndex"]
            last_station=stationList["LOCS"][i]
            next_station=stationList["LOCS"][i+1]
            deltaName=lineName+"_"+str(last_station["LABEL_NO"])+"_"+str(next_station["LABEL_NO"])
            local_time=time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())
            #print(last_index,next_index)

            #判断公交运行方向和标注的方向是否一致,两个站点间只有其中一个最小距离超出范围，则不计算
            if last_index>=next_index and (last_minDist>DIST_OFFSET or next_minDist>DIST_OFFSET):
                mon_db.bus_time_error.insert({"LINE_INFO":deltaName,"BUS_NO":busNo,"LABEL_TIME":label_time,"ERROR_TYPE":"OD","LOCSNUM":locsNum})
                continue

            #距离站点的最小距离小于MIN_DIST，即认为该时间为站点时间,若距离站点的最小距离大于MIN_DIST，则进行时间插值处理
            if last_minDist<MIN_DIST:
                last_site_time=datetime.strptime(locs[last_index]["site_time"],"%Y/%m/%d %H:%M:%S")
            else :
                #最小距离超过500米，说明已经汽车已经行驶一段时间了，那这个期间就不进行估计
                if last_minDist>DIST_OFFSET:
                    mon_db.bus_time_error.insert({"LINE_INFO":deltaName,"BUS_NO":busNo,"LABEL_TIME":label_time,"ERROR_TYPE":"LT","LOCSNUM":locsNum})
                    continue
                if last_index==0 or last_index==locsNum-1:
                    last_site_time=datetime.strptime(locs[last_index]["site_time"],"%Y/%m/%d %H:%M:%S")-timedelta(seconds=DEPARTURE_TIME_OFFSET)
                else:
                    last_site_time=interpolateStationTime(locs[last_index-1],locs[last_index+1],last_station)
            
            if next_minDist<MIN_DIST:
                next_site_time=datetime.strptime(locs[next_index]["site_time"],"%Y/%m/%d %H:%M:%S")
            else:
                if next_minDist>DIST_OFFSET:
                    mon_db.bus_time_error.insert({"LINE_INFO":deltaName,"BUS_NO":busNo,"LABEL_TIME":label_time,"ERROR_TYPE":"NT","LOCSNUM":locsNum})
                    continue
                if next_index==locsNum-1 or next_index==0:
                    next_site_time=datetime.strptime(locs[next_index]["site_time"],"%Y/%m/%d %H:%M:%S")+timedelta(seconds=DEPARTURE_TIME_OFFSET)
                else:
                    next_site_time=interpolateStationTime(locs[next_index-1],locs[next_index+1],next_station)
              
            #存储站点间的时间间隔(seconds)
            if last_site_time>next_site_time:
                continue
            delta_time=(next_site_time-last_site_time).seconds
            mon_db.bus_station_deltatime.insert({"line_info":deltaName,"delta_time":delta_time,"last_station_time":last_site_time.strftime("%Y/%m/%d %H:%M:%S"),"next_station_time":next_site_time.strftime("%Y/%m/%d %H:%M:%S"),"bus_no":busNo,"create_time":local_time})
        return timeDeltas
    except Exception as e:
        with open("bus_time_process.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t getTimeDelta occur error: "+str(e)+"\n")
               
def interpolateStationTime(last_loc,next_loc,station):
    """
    对到站时间进行插值，假设到站期间段为匀速
    input:
        last_loc:{"lng","lat","site_time"}
        next_loc:{"lng","lat","site_time"}
        station:{"LABEL_NO","LNG","LAT"}
    output:
        time
    """
    dist_ls=getDistanceWGS(float(last_loc["lng"]),float(last_loc["lat"]),float(station["LNG"]),float(station["LAT"]))
    dist_ns=getDistanceWGS(float(next_loc["lng"]),float(next_loc["lat"]),float(station["LNG"]),float(station["LAT"]))
    dist_ln=dist_ls+dist_ns
    last_time=datetime.strptime(last_loc["site_time"],"%Y/%m/%d %H:%M:%S")
    next_time=datetime.strptime(next_loc["site_time"],"%Y/%m/%d %H:%M:%S")
    delta=(next_time-last_time).seconds
    delta=int(delta*dist_ls/dist_ln)
    #print("时间插值:",delta)
    return last_time+timedelta(seconds=delta)

def printDist(dist):
    r,c=np.shape(dist)
    with open("dist.log","w+") as f:
        for i in range(r):
            for j in range(c):
                f.write("%f\t"%(dist[i,j]))
            f.write("\n")

def calcMinDistAndTimeDelta(locs,stationList,stationNum,busNo,label_time):
    """
    计算GPS点与站点之间的最小距离，然后根据不同站点之间的site_time求时间间隔

    input:
        locs:[{"lng","lat","site_time"},{},{}]
        stationList:{"LINE_NO","IS_UP_DOWN","LOCS":[{"LABEL_NO","LNG","LAT"},{}]}
    """
    locsNum=len(locs)
    dists=np.full((locsNum,stationNum),DIST_DELTA,dtype=float)
    for i, loc in enumerate(locs):
        for j,station in enumerate(stationList['LOCS']):
            dists[i,j]=getDistanceWGS(float(loc["lng"]),float(loc["lat"]),float(station["LNG"]),float(station["LAT"]))
    #printDist(dists)
    #获取最靠近站点的坐标index
    minIndexes=getMinIndexes(dists)
    #根据坐标index求站点之间的时间间隔
    getTimeDelta(locs,minIndexes,stationList,busNo,label_time)
    
def main():
    start_day=datetime(2019,7,10)
    d=date.today()
    end_day=datetime(d.year, d.month, d.day)

    bus_mid_info=mon_db.bus_mid_info     #存储中间结果
    bus_mid_split=mon_db.bus_mid_split   #存储班次分隔结果
    stationLineInfo=getStationList(mon_db)

    try:
        #先按天获取所有线路的中间数据
        while start_day<end_day:
            d=date.today()
            end_day=datetime(d.year, d.month, d.day)
            line_cursor=mon_db.kctest_line_info.find({},{"LINE_NO":1,"IS_UP_DOWN":1},no_cursor_timeout=True)
            label_time=start_day.strftime("%Y/%m/%d")
            
            start_time=time.time()

            for line in line_cursor:
                bus_mid_set=bus_mid_info.find({"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"],"LABEL_TIME":label_time},no_cursor_timeout=True)
                line_key=line["LINE_NO"]+"_"+str(line["IS_UP_DOWN"])
                #print(line_key,label_time)
                if len(stationLineInfo[line_key]["LOCS"])==0:
                    continue

                first_station=stationLineInfo[line_key]["LOCS"][0]   #始发站  {"LABEL_NO","LNG","LAT"}
                
                for item in bus_mid_set:
                    #利用站点之间的时间间隔来分隔每趟车每个班次
                    locs=item["LOCS"]   #[{"lng","lat","site_time"},{"lng","lat","site_time"},{"lng","lat","site_time"}]
                    busNo=item["BUS_NO"]
                    locsNum=len(locs)
                    split_index=[]
                    for i in range(locsNum-1):
                        last_time=datetime.strptime(locs[i]["site_time"],"%Y/%m/%d %H:%M:%S")
                        next_time=datetime.strptime(locs[i+1]["site_time"],"%Y/%m/%d %H:%M:%S")
                        delta=(next_time-last_time).seconds
                        if delta>LOOP_TIME_DELTA and line["LINE_NO"] in LOOP_LINE_NO:
                            #环线分割，分割点在起始站附近
                            first_station_dist=getDistanceWGS(float(locs[i]['lng']),float(locs[i]['lat']),float(first_station["LNG"]),float(first_station["LAT"]))
                            if first_station_dist<200:
                                split_index.append(i+1)
                        elif delta>TIME_DELTA:  
                            #当两点之间超过半个小时，且下一时刻比上一时刻更靠近始发站，且距离相差较大
                            first_station_dist1=getDistanceWGS(float(locs[i]['lng']),float(locs[i]['lat']),float(first_station["LNG"]),float(first_station["LAT"]))
                            first_station_dist2=getDistanceWGS(float(locs[i+1]['lng']),float(locs[i+1]['lat']),float(first_station["LNG"]),float(first_station["LAT"]))
                            if first_station_dist1>first_station_dist2+DIST_DELTA:
                                split_index.append(i+1)
                    split_index.append(locsNum)
                    
                    #分隔后计算站点之间时间差
                    stationNum=len(stationLineInfo[line_key]['LOCS'])
                    last_index=0
                    for i in range(len(split_index)):
                        next_index=split_index[i]
                        temp_locs=locs[last_index:next_index]
                        if len(temp_locs)<=20:
                            continue
                        bus_mid_split.insert({"LINE_NO":item["LINE_NO"],"IS_UP_DOWN":item["IS_UP_DOWN"],"BUS_NO":item["BUS_NO"],"LABEL_TIME":item["LABEL_TIME"],"LOCS":temp_locs})
                        calcMinDistAndTimeDelta(temp_locs,stationLineInfo[line_key],stationNum,busNo,label_time)
                        last_index=next_index       
                #记录量车每天跑了多少趟及总耗时
            start_day += timedelta(days=1)

            with open("bus_time_process.log","a+") as f:
                f.write("get bus time delta on:%s, cost time:%f\n"%(label_time,time.time()-start_time))

            if start_day>=end_day:
                with open("bus_time_process.log","a+") as f:
                    f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t time sleep!"+"\n")
                time.sleep(3600*24)
                d=date.today()
                end_day=datetime(d.year, d.month, d.day)
    except Exception as e:
        with open("bus_time_process.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t main occur error: "+str(e)+"\n")

if __name__=="__main__":
    main()
    #lineStationInfo=getStationList()
    #print(lineStationInfo["6_0"]["LOCS"][-1])

