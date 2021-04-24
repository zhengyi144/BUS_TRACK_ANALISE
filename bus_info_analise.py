#coding=utf-8
from pymongo import MongoClient,ASCENDING
import numpy as np
import time
import os
from datetime import datetime,date,timedelta
from bson.code import Code

mon_ipAdrr="10.10.116.200"
mon_port=27017
mon_conn=MongoClient(mon_ipAdrr,mon_port)
mon_db=mon_conn.kctest
start_day=datetime(2019,7,3,6)
end_day=datetime(2019,7,4,6)

bus_mid_set=mon_db.bus_mid_info     #存储中间结果

try:
    while start_day<end_day:
        d=date.today()
        end_day=datetime(d.year, d.month, d.day,6)
        line_cursor=mon_db.kctest_line_info.find({},{"LINE_NO":1,"IS_UP_DOWN":1})
        
        #先按天查询每条线路的实时交通信息，并按照车次分组
        begin_time=start_day.strftime("%Y/%m/%d %H:%M:%S")
        end_time=(start_day+timedelta(days=1)).strftime("%Y/%m/%d %H:%M:%S")
        label_time=start_day.strftime("%Y/%m/%d")
        for line in line_cursor:
            #查询每条线路站点信息
            #station_cursor=mon_db.kctest_station_line_info.find({"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"]}).sort([("LABEL_NO",1)])
            mapper=Code(""" 
                        function(){
                            emit(this.BUS_NO,{lng:this.LNG,lat:this.LAT,site_time:this.SITE_TIME});
                        };
                        """)
            reducer=Code(""" 
                        function(key,values){
                            var arr=[];
                            for(var i=0;i<values.length;i++){
                                arr.push(values[i]);
                            }
                            return {info:arr};
                        };
                        """)
            
            #map_reduce分组不太准确，可能同一个id存在多个分组
            #db.bus_run_info.find({"LINE_NO":"6","IS_UP_DOWN":0,"BUS_NO":20856,"RUN_STATE":1,"SITE_TIME":{"$lt":"2019/07/04 00:00:00"}})
            result=mon_db.bus_run_info.map_reduce(mapper,reducer,out="bus_info_set",query={"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"],"RUN_STATE":1,"SITE_TIME":{"$gte":begin_time,"$lt":end_time}})
            
            #处理每个分组，并对GPS坐标按照时间进行排序
            for group in mon_db.bus_info_set.find():
                locs=[]
                if "info" not in group["value"]:
                    continue
                for records in group["value"]["info"]:
                    if "info" in records:
                        for item in records["info"]:
                            #item["site_time"]=datetime.strptime(item["site_time"],"%Y/%m/%d %H:%M:%S")
                            locs.append(item)
                    else:
                        locs.append(records)
                locs.sort(key=lambda x:x["site_time"])
                
                #先进行中间数据存储
                bus_mid_set.insert({"LINE_NO":line["LINE_NO"],"IS_UP_DOWN":line["IS_UP_DOWN"],"BUS_NO":group["_id"],"LOCS":locs,"LABEL_TIME":label_time,"LOCS_COUNT":len(locs)})
        start_day += timedelta(days=1)

        with open("bus_middle.log","a+") as f:
            f.write("one day bus middle info begin_time:%s, end_time:%s\n"%(begin_time,end_time))
        
        if start_day>=end_day:
            with open("bus_middle.log","a+") as f:
                f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t time sleep!"+"\n")
            time.sleep(3600*24)
            d=date.today()
            end_day=datetime(d.year, d.month, d.day,6)
except Exception as e:
    with open("bus_middle.log","a+") as f:
        f.write("bus middle info except:%s\n"%(str(e)))


        