#coding=utf-8
from pymongo import MongoClient,ASCENDING
import numpy as np
import time
import os
from datetime import datetime,date,timedelta
from utils.getStations import getStationList
#from bson.code import Code
from utils.GPSConvertUtils import *
from bus_time_process import getMinIndexes

"""
获取各个公交站间的公交路段上的GPS点，用于后续判断车辆所属路段
"""
mon_ipAdrr="10.10.116.200"
mon_port=27017
mon_conn=MongoClient(mon_ipAdrr,mon_port)
mon_db=mon_conn.kctest

MIN_DIST=200

def calcPointStationDist(locs,stations):
    """
    input:
        locs:[{"lng","lat","site_time"},{},{}]
        stations:{"LINE_NO","IS_UP_DOWN","LOCS":[{"LABEL_NO","LNG","LAT"},{}]}
    """
    locsNum=len(locs)
    stationNum=len(stations["LOCS"])
    dists=np.full((locsNum,stationNum),10000,dtype=float)
    for i, loc in enumerate(locs):
        for j,station in enumerate(stations['LOCS']):
            dists[i,j]=getDistanceWGS(float(loc["lng"]),float(loc["lat"]),float(station["LNG"]),float(station["LAT"]))
    return dists

def saveRoadPoints(line_id,locs,stations,minIndexes):
    """
    line_id:'6_0'
    locs:[{"lng","lat","site_time"},{},{}]
    stations:{"LINE_NO","IS_UP_DOWN","LOCS":[{"LABEL_NO","LNG","LAT"},{}]}
    minIndexes:[{'minDist','rIndex','cIndex'},{},{}], len(minIndexes)=stationNum, 'rIndex','cIndex'分别是dists的位置索引
    """
    stationNum=len(minIndexes)
    for i in range(stationNum-1):
        last_minDist=minIndexes[i]["minDist"]
        last_index=minIndexes[i]["rIndex"]
        start_label=minIndexes[i]["cIndex"]+1
        next_minDist=minIndexes[i+1]["minDist"]
        next_index=minIndexes[i+1]["rIndex"]
        end_label=minIndexes[i+1]["cIndex"]+1
        
        #判断公交运行方向和标注的方向是否一致,两个站点间只有其中一个最小距离超出范围，则不计算
        if last_index>=next_index or last_minDist>MIN_DIST or next_minDist>MIN_DIST:
            continue
        
        #存储站点之间的点作为路段
        line_info=line_id+"_"+str(start_label)+"_"+str(end_label)
        temp_locs=locs[last_index+1:next_index]
        #print(temp_locs)
        if len(temp_locs)==0:
            continue
        first_dist=getDistanceWGS(float(temp_locs[0]["lng"]),float(temp_locs[0]["lat"]),float(stations['LOCS'][i]['LNG']),float(stations['LOCS'][i]['LAT']))
        last_dist=getDistanceWGS(float(temp_locs[-1]["lng"]),float(temp_locs[-1]["lat"]),float(stations['LOCS'][i+1]['LNG']),float(stations['LOCS'][i+1]['LAT']))
        temp_dist=first_dist+last_dist
        label_time=temp_locs[0]['site_time'].split(" ")[-1]
        for i in range(len(temp_locs)-1):
            temp_dist+=getDistanceWGS(float(temp_locs[i]["lng"]),float(temp_locs[i]["lat"]),float(temp_locs[i+1]["lng"]),float(temp_locs[i+1]["lat"]))

        item={"LINE_INFO":line_info,
             "LOCS":temp_locs,
             "ESTIMATE_DIST":temp_dist,
             "LABEL_TIME":label_time}
        mon_db.station_road_points.update({"LINE_INFO":item["LINE_INFO"],"LABEL_TIME":item["LABEL_TIME"]},{'$set':item},True)


def getRoadPoints():
    """
    获取两站点之间GPS点集
    """
    stationList=getStationList(mon_db)
    label_time="2019/07/11"
    line_cursor=mon_db.kctest_line_info.find({},{"LINE_NO":1,"IS_UP_DOWN":1},no_cursor_timeout=True)
    for line in line_cursor:
        line_id=line["LINE_NO"]+"_"+str(line["IS_UP_DOWN"])
        if line_id!="13_1":
            continue
        midpoints_cursor=mon_db.bus_mid_split.find({"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"],"LABEL_TIME":label_time})
        stations=stationList[line_id]
        for midpoints in midpoints_cursor:
            #先取11-4时段的点
            locs=midpoints["LOCS"]
            if locs[0]["site_time"]>"2019/07/11 11:00:00" and locs[0]["site_time"]<"2019/07/11 16:00:00":
                dists=calcPointStationDist(locs,stations)
                minIndexes=getMinIndexes(dists)
                saveRoadPoints(line_id,locs,stations,minIndexes)

def calcMeanEstimateDist(line_info):
    cursor=mon_db.station_road_points.find({"LINE_INFO":line_info},{"ESTIMATE_DIST":1})
    dists=[]
    for dist in cursor:
        dists.append(dist["ESTIMATE_DIST"])
    if len(dists)>0:
        return np.mean(dists)
    else:
        return 0

def calcStationDist():
    """
    计算两站点之间直线距离及估计距离
    """
    stationList=getStationList(mon_db)
    line_cursor=mon_db.kctest_line_info.find({},{"LINE_NO":1,"IS_UP_DOWN":1},no_cursor_timeout=True)
    for line in line_cursor:
        line_id=line["LINE_NO"]+"_"+str(line["IS_UP_DOWN"])
        if line_id!="13_1":
            continue
        stations=stationList[line_id]["LOCS"]
        for i in range(len(stations)-1):
            last_station=stations[i]
            next_station=stations[i+1]
            line_info=line_id+"_"+str(last_station["LABEL_NO"])+"_"+str(next_station["LABEL_NO"])
            LINEAR_DIST=getDistanceWGS(float(last_station["LNG"]),float(last_station["LAT"]),float(next_station["LNG"]),float(next_station["LAT"]))
            ESTIMATE_DIST=calcMeanEstimateDist(line_info)
            item={
                 "LINE_INFO":line_info,
                 "LINEAR_DIST":LINEAR_DIST,
                 "ESTIMATE_DIST":ESTIMATE_DIST,
                 "AMAP_DIST":0}
            mon_db.station_road_dist.update({"LINE_INFO":item["LINE_INFO"]},{'$set':item},True)

def getExceptRoadPoints():
    """
    获取异常的路段点集：
    1、路段分隔错误
    2、缺历史数据
    """
    cursor=mon_db.station_road_dist.find({"ESTIMATE_DIST":{"$lt":10}})
    for item in cursor:
        line_info=item["LINE_INFO"].split("_")
        print(line_info[0],line_info[1],line_info[2],line_info[3])
        

if __name__=="__main__":
    getRoadPoints()
    calcStationDist()
    getExceptRoadPoints()