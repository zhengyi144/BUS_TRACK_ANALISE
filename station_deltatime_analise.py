#coding=utf-8
from pymongo import MongoClient
import numpy as np
import time
import os
from datetime import datetime,date,timedelta
from utils.getStations import getStationList
from utils.getHolidayUtils import getHoliday
import pandas as pd

mon_ipAdrr="10.10.116.200"
mon_port=27017
mon_conn=MongoClient(mon_ipAdrr,mon_port)
mon_db=mon_conn.kctest
stationList=getStationList(mon_db)

def getOneHourTimeList():
    startTime=datetime(2019,7,3,6)
    #print(startTime.hour)
    hourList=[]
    while startTime.hour<23:
        nextTime=startTime+timedelta(hours=1)
        startTimeStr=startTime.strftime("%Y/%m/%d %H:%M:%S").split(" ")
        nextTimeStr=nextTime.strftime("%Y/%m/%d %H:%M:%S").split(" ")
        startTime=nextTime
        hourList.append({"start_time":startTimeStr[-1],"next_time":nextTimeStr[-1],"htimes":[],"wtimes":[]})
    return hourList

def analiseDeltaTime(timeArr):
    """
     利用百分位剔除异常值，提取50%及75%分位数
    """
    data=pd.DataFrame(timeArr,columns=["station_time"])
    data=data["station_time"]
    ds=data.describe()
    #print(ds)
    p2=ds["50%"]
    p3=ds["75%"]
    return p2,p3


def clusterStationTimeInterval():
    # start_day=datetime(2019,7,3)
    # d=date.today()
    # end_day=datetime(d.year, d.month, d.day)
    hourList=getOneHourTimeList()
    #print(hourList)

    line_cursor=mon_db.kctest_line_info.find({},{"LINE_NO":1,"IS_UP_DOWN":1},no_cursor_timeout=True)
    for line in line_cursor:
        line_key=line["LINE_NO"]+"_"+str(line["IS_UP_DOWN"])
        #print(line_key,label_time)
        if  len(stationList[line_key]["LOCS"])==0:
            continue
        station_locs=stationList[line_key]["LOCS"]
        for i in range(len(station_locs)-1):
            line_info=line_key+"_"+str(station_locs[i]["LABEL_NO"])+"_"+str(station_locs[i+1]["LABEL_NO"])
            # if line_info!="6_0_14_15":
            #     continue
            stationTime_cursor=mon_db.bus_station_deltatime.find({"line_info":line_info})
            #先清空数据
            for n in range(len(hourList)):
                hourList[n]["htimes"]=[]
                hourList[n]["wtimes"]=[]
        
            for item in stationTime_cursor:
                last_station_time=item["last_station_time"]
                next_station_time=item["next_station_time"]
                 #对于特殊情况就不进行统计了
                if last_station_time>next_station_time:
                    continue
               
                #根据last_station_time划分工作日、节假日，再按照一个小时的粒度统计站点之间时间间隔
                dayTime=last_station_time.split(" ")
                last_time=dayTime[-1]
                isHoliday=getHoliday(dayTime[0])
                if isHoliday==-1:
                    continue
                
                for n in range(len(hourList)):
                    #print(last_time,hourList[n]["start_time"])
                    if last_time>=hourList[n]["start_time"] and last_time<hourList[n]["next_time"]:
                        if isHoliday==1:
                            hourList[n]["htimes"].append(item["delta_time"])
                        else:
                            hourList[n]["wtimes"].append(item["delta_time"])
                        break
            #print(hourList)
            #开始分析各个时段的数据
            for hourItem in hourList:
                htimesArr=hourItem["htimes"]
                wtimesArr=hourItem["wtimes"]
                if len(htimesArr)>0:
                    hp2,hp3=analiseDeltaTime(htimesArr)
                else:
                    hp2=0
                    hp3=0

                if len(wtimesArr)>0:
                    wp2,wp3=analiseDeltaTime(wtimesArr)
                else:
                    wp2=0
                    wp3=0
                
                result_item={"line_info":line_info,
                             "start_time":hourItem["start_time"],
                             "next_time":hourItem["next_time"],
                             "htime_50":hp2,
                             "htime_75":hp3,
                             "wtime_50":wp2,
                             "wtime_75":wp3}
                #print(result_item)
                #存储各个时段的百分位数
                mon_db.station_time_result.update({"line_info":result_item["line_info"],"start_time":result_item["start_time"],"next_time":result_item["next_time"]},{'$set':result_item},True)

if __name__=="__main__":
    clusterStationTimeInterval()