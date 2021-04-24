#coding=utf-8

def getStationList(mon_db):
    """
       return {"6_0":{"LINE_NO","IS_UP_DOWN","LOCS":[{"LABEL_NO","LNG","LAT"},{}]},"6_1":{...}}
    """
    line_cursor=mon_db.kctest_line_info.find({},{"LINE_NO":1,"IS_UP_DOWN":1})
    stationList=[]
    stationName=[]
    for line in line_cursor:
        #查询每条线路站点信息
        station_cursor=mon_db.kctest_station_line_info.find({"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"]}).sort([("LABEL_NO",1)])
        locs=[]
        for station in station_cursor:
            locs.append({"LABEL_NO":station["LABEL_NO"],"LNG":station["LNG"],"LAT":station["LAT"]})
        stationList.append({"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"],"LOCS":locs})
        stationName.append(line["LINE_NO"]+"_"+str(line["IS_UP_DOWN"]))
    stationLineInfo=dict(zip(stationName,stationList))
    return stationLineInfo
