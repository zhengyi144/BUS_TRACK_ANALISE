#coding=utf-8
from pymongo import MongoClient
import numpy as np
import time
import os
import math
from datetime import datetime,date,timedelta
from utils.getStations import getStationList
from utils.getHolidayUtils import getHoliday
from bson.son import SON
from apscheduler.schedulers.background import BackgroundScheduler
from utils.GPSConvertUtils import *


mon_ipAdrr="10.10.116.200"
mon_port=27017
mon_conn=MongoClient(mon_ipAdrr,mon_port)
mon_db=mon_conn.kctest
stationList=getStationList(mon_db)

ARRIVAL_DIST=100  #小于80米表示到站
COMING_DIST=200 #小于200米表示即将到站

"""
实时公交到站时间测试
"""
def getRealTimeBusInfo(line_no,is_up_down,begin_time,end_time):
    """
    抓取line_no路线上最新的实时公交数据,条件根据site_time
    input:
         line_no:"6"
         is_up_down:0/1
         begin_time:"2019/07/23 09:05:00"
         end_time:"2019/07/23 09:05:00"
    """
    mon_db.temp_bus_run_info.remove({})
    bus_run_info_cur=mon_db.kctest_bus_run_info.find({"LINE_NO":line_no,"IS_UP_DOWN":is_up_down,"RUN_STATE":1,"CREATE_DATE":{"$gte":begin_time,"$lt":end_time}})
    for item in bus_run_info_cur:
        #插入临时表
        mon_db.temp_bus_run_info.update({"SITE_TIME":item["SITE_TIME"],"BUS_NO":item["BUS_NO"]},{'$set':item},True)
    
    bus_info=mon_db.temp_bus_run_info.find()
    return bus_info

def getMinDist(bus_info,road_points):
    """
     bus_info:{"LINE_NO","BUS_NO","LNG","LAT","SITE_TIME",...}
     road_points:[{"lng","lat","site_time"},{},{}]
    """
    min_dist=1000
    for point in road_points:
        dist=getDistanceWGS(float(bus_info["LNG"]),float(bus_info["LAT"]),float(point["lng"]),float(point["lat"]))
        if min_dist>dist:
            min_dist=dist
    return min_dist

def calcNextStationDist(bus_info,road_info):
    """
     bus_info:{"LINE_NO","BUS_NO","LNG","LAT","SITE_TIME",...}
     road_info:{"LINE_INFO","LABEL_TIME","LOCS":[{"lng","lat","site_time"},{},{}],"ESTIMATE_DIST"}
    """
    dists=[]
    road_points=road_info["LOCS"]
    for point in road_points:
        dists.append(getDistanceWGS(float(bus_info["LNG"]),float(bus_info["LAT"]),float(point["lng"]),float(point["lat"])))
    minIndex=np.argmin(dists)
    
    sum_dist=0
    for i in range(minIndex,len(road_points)-1):
        sum_dist+=getDistanceWGS(float(road_points[i]["lng"]),float(road_points[i]["lat"]),float(road_points[i+1]["lng"]),float(road_points[i+1]["lat"]))
    return sum_dist

def calcStationRoad(line_key,bus_info,dist,stations):
    """
    计算GPS点位所属的路段，与历史位置进行比较
    """
    next_min_index=np.argmin(dist)
    station_num=len(stations)
    if next_min_index==0 or next_min_index==station_num-1:
        #说明车在1-->2站点之间或者已至终点站
        return next_min_index,next_min_index+1,
    else:
        last_station=stations[next_min_index-1]
        current_station=stations[next_min_index]
        next_station=stations[next_min_index+1]
        next_line_info=line_key+"_"+str(current_station['LABEL_NO'])+"_"+str(next_station["LABEL_NO"])
        last_line_info=line_key+"_"+str(last_station['LABEL_NO'])+"_"+str(current_station["LABEL_NO"])
        print(last_line_info,next_line_info)
        last_road=mon_db.station_road_points.find({"LINE_INFO":last_line_info}).limit(0)[0]
        next_road=mon_db.station_road_points.find({"LINE_INFO":next_line_info}).limit(0)[0]

        #将GPS点与两个路段上的所有GPS点集进行比较，距离最小的点所在路段
        last_min_dist=getMinDist(bus_info,last_road["LOCS"])
        next_min_dist=getMinDist(bus_info,next_road["LOCS"])

        if last_min_dist<next_min_dist:
            return next_min_index-1,next_min_index
        else:
            return next_min_index,next_min_index+1

def calcNearestStation(line_key,bus_info,stations):
    """
      line_key:"6_0"
      bus_info:{"LINE_NO","BUS_NO","LNG","LAT","SITE_TIME",...}
      stations:[{"LABEL_NO","LNG","LAT"},{}]
      last_bus_info:{"BUS_NO","LNG","LAT","SITE_TIME","DIST"}
    """
    dist=[]
    for station in stations:
        dist.append(getDistanceWGS(float(bus_info["LNG"]),float(bus_info["LAT"]),float(station["LNG"]),float(station["LAT"])))
    last_station_index,next_station_index=calcStationRoad(line_key,bus_info,dist,stations)
    return {"last_station_index":last_station_index,"next_station_index":next_station_index,"dist":dist}

def getLaterStationTimes(line_no,now_time,start_station_index,stations,isHoliday,mode):
    """
      获取剩余站点的时间集合
      line_no:"6_0"
      now_time:datetime
      start_station_index:起始站点index
      stations：[{"LABEL_NO","LNG","LAT"},{}]
      isHoliday:0 workdays/1 holidays
      mode:0 represent time_50,1 represent time_75
    """
    start_time=now_time
    stationTimeList=[]
    for i in range(start_station_index,len(stations)-1):
        last_station=stations[i]["LABEL_NO"]
        next_station=stations[i+1]["LABEL_NO"]
        line_info=line_no+"_"+str(last_station)+"_"+str(next_station)
        #print(line_info)
        time_info=start_time.strftime("%Y/%m/%d %H:%M:%S").split(" ")[-1]
        station_time_cursor=mon_db.station_time_result.find({"line_info":line_info,"start_time":{"$lte":time_info},"next_time":{"$gt":time_info}})
        if station_time_cursor.count()>1:
            print("find station time result excepted!")
        for item in station_time_cursor:
            station_time=0
            if isHoliday:
                if mode=='0':
                    station_time=math.ceil(item["htime_50"])
                else:
                    station_time=math.ceil(item["htime_75"])
            else:
                if mode=='0':
                    station_time=math.ceil(item["wtime_50"])
                else:
                    station_time=math.ceil(item["wtime_75"])
            stationTimeList.append(station_time)
            start_time=start_time+timedelta(seconds=station_time)
            #print(station_time,start_time)
    return stationTimeList

def getLaterStationDists(line_no,start_station_index,stations):
    """
    获取从start_station_index到终点站的之间距离
    line_no:"6_0"
    start_station_index:1
    stations:[{"LABEL_NO","LNG","LAT"},{}]
    """
    stationDistList=[]
    for i in range(start_station_index,len(stations)-1):
        line_info=line_no+"_"+str(stations[i]["LABEL_NO"])+"_"+str(stations[i+1]["LABEL_NO"])
        station_dist=mon_db.station_road_dist.find({"LINE_INFO":line_info})[0]
        dist=station_dist["AMAP_DIST"] if station_dist["AMAP_DIST"]>0 else (station_dist["ESTIMATE_DIST"] if station_dist["ESTIMATE_DIST"]>0 else station_dist["LINEAR_DIST"])
        stationDistList.append(dist)
    return stationDistList 

def calcArrivalStationTimeAndDist(bus_info,pointInfo,stations,line_no,time_info,isHoliday,mode='0'):
    """
      计算该车行驶至各站的到站时间及到站距离
      bus_info:{"LINE_NO","BUS_NO","LNG","LAT","SITE_TIME",...}
      pointInfo:{"last_station_index","next_station_index"}
      stations：[{"LABEL_NO","LNG","LAT"},{}]
      line_no:"6_0"
      time_info:"12:56:00"
      isHoliday:True/False
      mode:0 represent time_50,1 represent time_75
    """
    now_time=datetime.now()  #time.localtime()
    #print(now_time)
    #对于初始点是路段中间点时，根据该点至下一站的距离与站点之间距离的比值*stationtime
    last_station_index=pointInfo["last_station_index"]
    next_station_index=pointInfo["next_station_index"]
    
    last_station_dist=pointInfo["dist"][last_station_index]
    next_station_dist=pointInfo["dist"][next_station_index]
    
    #计算到各站之间的时间及距离
    stationArrivingTimeAndDist=[]

    #获取站点间的时间间隔
    stationTimeList=getLaterStationTimes(line_no,now_time,last_station_index,stations,isHoliday,mode)
    #获取站点间的距离
    stationDistList=getLaterStationDists(line_no,last_station_index,stations)
 
    #已到站就不统计时间了，未到站则按照距离和速度进行微调
    if next_station_dist<ARRIVAL_DIST:
        stationArrivingTimeAndDist.append({"label_no":next_station_index+1,"atime":0,"dist":0})
    else:
        line_info=line_no+"_"+str(last_station_index+1)+"_"+str(next_station_index+1)
        road_info=mon_db.station_road_points.find({"LINE_INFO":line_info}).limit(0)
        if road_info.count()>0:
            rest_dist=calcNextStationDist(bus_info,road_info[0])
            station_dist_ratio=rest_dist/road_info["ESTIMATE_DIST"]
        else:
            rest_dist=next_station_dist
            station_dist_ratio=rest_dist/stationDistList[0]
        station_time=stationTimeList[0]*station_dist_ratio
        stationArrivingTimeAndDist.append({"label_no":int(next_station_index+1),"atime":int(math.ceil(station_time)),"dist":int(math.ceil(rest_dist))})
    
    #统计后续各个站点的时间
    for i in range(1,len(stationTimeList)):
        last_station_time=stationArrivingTimeAndDist[i-1]["atime"]
        last_station_dist=stationArrivingTimeAndDist[i-1]["dist"]
        stationArrivingTimeAndDist.append({"label_no":int(next_station_index+1+i),"atime":int(math.ceil(last_station_time+stationTimeList[i])),"dist":int(math.ceil(last_station_dist+stationDistList[i]))})

    return stationArrivingTimeAndDist

def printStationArrivingTime(stationArrivingTime,pointInfo,line_no,is_up_down,bus_no):
    """
      stationArrivingTime:[{"label_no","atime","dist"}]
      pointInfo:{"last_station_index","next_station_index"}
      line_no:'6'
      is_up_down:0 or 1
    """
    start_label_no=pointInfo["next_station_index"]+1
    station_info_cursor=mon_db.kctest_station_line_info.find({"LINE_NO":line_no,"IS_UP_DOWN":is_up_down,"LABEL_NO":{"$gte":int(start_label_no)}}).sort([("LABEL_NO",1)])
    line=str(bus_no)+"到达各站点时间："
    if station_info_cursor.count()!=len(stationArrivingTime):
        print("remain station list error!")
        return

    for idx,item in enumerate(station_info_cursor):
        #将时间改为分钟形式，向上取整
        stationTime=math.ceil(stationArrivingTime[idx]["atime"]/60)
        stationDist=stationArrivingTime[idx]["dist"]
        if stationTime==0:
            stationTime="已到站 "
        elif stationTime<=1:
            stationTime="1分钟内到站/"+str(stationDist)+"米 "
        else:
            stationTime="约"+str(stationTime)+"分钟/"+str(stationDist)+"米 "
        line+=item["STATION_NAME"]+":"+stationTime
    print(line)

def main():
    line_no='18'
    is_up_down=1
    line_info=mon_db.kctest_line_info.find({"LINE_NO":line_no,"IS_UP_DOWN":is_up_down})
    print(line_info[0]["STATION_FIRST"],line_info[0]["STATION_LAST"])
    line_key=line_no+"_"+str(is_up_down)
    stationList=getStationList(mon_db)
    #strptime("2019/07/05 08:00:00","%Y/%m/%d %H:%M:%S")

    while True:
        #获取该路线上的公交车数据
        """start_time=datetime.now()-timedelta(minutes=2)
        begin_time=(start_time-timedelta(seconds=30)).strftime("%Y/%m/%d %H:%M:%S")
        print(begin_time)
        end_time=start_time.strftime("%Y/%m/%d %H:%M:%S")
        bus_infos=getRealTimeBusInfo(line_no,is_up_down,begin_time,end_time)"""

        bus_infos=[{
        "LINE_NO":line_no,
        "LNG":119.281283333,
        "LAT":26.072966666666666,
        "SITE_TIME":"2019/08/02 15:56:01",
        "BUS_NO":"1235"
        }]
        begin_time="2019/08/02 15:56:01"
        #确认时间段
        time_info=begin_time.split(" ")
        isHoliday=getHoliday(time_info[0])
        stations=stationList[line_key]["LOCS"]
        for bus_info in bus_infos:
            #计算各趟公交车处于哪个路段
            pointInfo=calcNearestStation(line_key,bus_info,stations)
            #已知公交车处于哪个路段后，开始计算到各站点的到站时间
            stationArrivingTime=calcArrivalStationTimeAndDist(bus_info,pointInfo,stations,line_key,time_info,isHoliday)

            printStationArrivingTime(stationArrivingTime,pointInfo,line_no,is_up_down,bus_info["BUS_NO"])
            
            """
            后续逐步优化
            将上一时刻的数据存在数据库中:
            RECORD_ID,LS_INDEX,NS_INDEX,lng,lat,site_time,arriving_next_time,OFFSET_LABEL
            OFFSET_LABEL:表示是否偏离的GPS点,True表示偏离，FALSE不偏离
            """
            last_info={
                "RECORD_ID":line_key+"_"+str(bus_info["BUS_NO"]),
                "LS_INDEX":int(pointInfo["last_station_index"]),
                "NS_INDEX":int(pointInfo["next_station_index"]),
                "LNG":bus_info["LNG"],
                "LAT":bus_info["LAT"],
                "SITE_TIME":bus_info["SITE_TIME"],
                "ARR_NEXT_TIME":stationArrivingTime[0],
                "ARR_NEXT_DIST":pointInfo["dist"][pointInfo["next_station_index"]],
                "OFFSET_LABEL":False
            }
            print(last_info)
            #mon_db.bus_arriving_station_time.update({"RECORD_ID":last_info["RECORD_ID"],"SITE_TIME":last_info["SITE_TIME"]},{'$set':last_info},True)

        #start_time=start_time+timedelta(seconds=30)
        time.sleep(25)

if __name__=="__main__":
    main()
    