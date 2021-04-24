#coding=utf-8
import cx_Oracle as oracle
from pymongo import MongoClient
import numpy as np
import time
import os
from datetime import datetime,date,timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor,ProcessPoolExecutor
from bson.objectid import ObjectId

#timer task read bus info from oracle database save to mongodb

#oracle database info
ora_ipAddr="120.78.155.33"
ora_userName="kctest"
ora_password="kctest123"
ora_port="1521"
ora_service="orcl"

#mongodb database info
mon_ipAdrr="10.10.116.200"
mon_port=27017


def readWriteLineInfo(ora_db,mon_db):
    try:
        #create oracle cursor
        cursor=ora_db.cursor()
        cursor.execute("select i.LINE_NO,i.IS_UP_DOWN,i.STATION_FIRST,i.STATION_LAST,i.FIRST_BUS_TIME,i.LAST_BUS_TIME,i.CARFARE from kctest_line_info i")
        dataset=cursor.fetchall()
        
        #create mongodb table
        mon_set=mon_db.kctest_line_info        #create set
        
        for data in dataset:
            mon_set.insert({
                "LINE_NO":data[0],
                "IS_UP_DOWN":data[1],
                "STATION_FIRST":data[2],
                "STATION_LAST":data[3],
                "FIRST_BUS_TIME":data[4],
                "LAST_BUS_TIME":data[5],
                "CARFARE":data[6]
            })
        print(mon_set.find().count())
        cursor.close()
    except Exception as e:  
        print(e)

def readWriteStationLineInfo(ora_db,mon_db):
    try:
        #create oracle cursor
        cursor=ora_db.cursor()
        cursor.execute("select LINE_NO,IS_UP_DOWN,LABEL_NO,STATION_NAME,LNG,LAT from kctest_station_line_info")
        dataset=cursor.fetchall()
        
        #create mongodb table
        mon_set=mon_db.kctest_station_line_info       #创建集合
        
        for data in dataset:
            mon_set.insert({
                "LINE_NO":data[0],
                "IS_UP_DOWN":data[1],
                "LABEL_NO":data[2],
                "STATION_NAME":data[3],
                "LNG":data[4],
                "LAT":data[5]
            })
        print(mon_set.find().count())
        cursor.close()
    except Exception as e:  
        print(e)

def reconnect_oracle():
    try:
        global ora_db
        ora_db=oracle.connect(ora_userName+"/"+ora_password+"@"+ora_ipAddr+":"+ora_port+"/"+ora_service)
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t ORACLE try reconnect!\n")
    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t ORACLE reconnect fail!"+str(e)+"\n")

def check_oracle_connect():
    try:
        cursor=ora_db.cursor()
        cursor.execute("select 1 from dual")
        cursor.close()
        return 1
    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t ORACLE connect occur exceptions:"+str(e)+"\n")
        return 0

def readWriteBusRunInfo():
    hour=datetime.now().hour
    if hour<6 or hour>23:
        return
    try:
        if check_oracle_connect()==0:
            reconnect_oracle()
        start_time=time.time()
        d=date.today()
        today=datetime(d.year, d.month, d.day)
        #create oracle cursor
        cursor=ora_db.cursor()
        cursor.execute("select LINE_NO,BUS_NO,BUS_NO_CHAR,IS_UP_DOWN,LNG,LAT,SITE_TIME,RUN_STATE,WILL_RUN from kctest_bus_run_info where is_up_down in (1,0) and run_state=1 and site_time>=:today",{"today":today})
        #dataset=cursor.fetchmany(2)
        
        #create mongodb table
        mon_set=mon_db.kctest_bus_run_info       
        
        items=[]
        for data in cursor:
            local_time=time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())
            if data[0] is None or data[1] is None or data[3] is None or data[4] is None or data[5] is None or data[6] is None:
                continue
            site_time= data[6].strftime("%Y/%m/%d %H:%M:%S")
            
            item={"LINE_NO":data[0],
                "BUS_NO":data[1],
                "BUS_NO_CHAR":data[2],
                "IS_UP_DOWN":data[3],
                "LNG":float(data[4]),
                "LAT":float(data[5]),
                "SITE_TIME":site_time,
                "RUN_STATE":data[7],
                "WILL_RUN":data[8],
                "CREATE_DATE":local_time,
                "REMOVE_LABEL":0}
            
            items.append(item)
            #mon_set.update({'LINE_NO':item["LINE_NO"],'BUS_NO':item["BUS_NO"],"SITE_TIME":item["SITE_TIME"]},{'$set':item},True)
        cursor.close()
        
        mon_set.insert_many(items)
        #print(mon_set.find().count())
        
        with open("readDataInfo.log","a+") as f:
            f.write("local time:%s, insert %d bus info time-consuming:%f\n"%(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()),len(items),time.time()-start_time))

    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t readWriteBusRunInfo occur exceptions:"+str(e)+"\n")
            if "ORA-" in str(e):
                ora_db.close()
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t readWriteBusRunInfo close ora_db!\n")

def statistics():
    try:
        local_time=time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())
        d=date.today()
        today=datetime(d.year, d.month, d.day)
        today=today.strftime("%Y/%m/%d %H:%M:%S")
        
        src_set=mon_db.kctest_bus_run_info
        dst_set=mon_db.bus_run_info
        #deal remain data, and release mongodb storage
        src_items=mon_db.kctest_bus_run_info.find({"SITE_TIME":{"$gte":today}})
        if src_items.count()>0:
            for item in src_items:
                id=item["_id"]
                del item["_id"]
                dst_set.update({'LINE_NO':item["LINE_NO"],'BUS_NO':item["BUS_NO"],"SITE_TIME":item["SITE_TIME"]},
                            {'$set':item},True)
                src_set.remove({"_id":ObjectId(id)})
            
        src_set.remove({})
        #release mongodb remove storage
        ret=os.popen('sh repairDatabase.sh')
        command=ret.read()
        ret.close()

        count=mon_db.bus_run_info.find({"SITE_TIME":{"$gte":today}}).count()
        with open("statistics.log","a+") as f:
            f.write("local time:%s, today:%s ,insert %d bus info records,shell command output:%s\n"%(local_time,today,count,command))
        
        #close oracle
        ora_db.close()
    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+local_time+"\t statistics occur exceptions:"+str(e)+"\n")

def distinctBusInfo():
    try:
        hour=datetime.now().hour
        if hour>=23 or hour<6:
            return
        start_time=time.time()
        begin_time=(datetime.now()-timedelta(seconds=30)).strftime("%Y/%m/%d %H:%M:%S")
        end_time=datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        src_set=mon_db.kctest_bus_run_info
        dst_set=mon_db.bus_run_info
        
        src_items=src_set.find({"CREATE_DATE":{"$gte":begin_time,"$lt":end_time},"REMOVE_LABEL":0})
        count=src_items.count()
        for item in src_items:
            id=item["_id"]
            del item["_id"]
            dst_set.update({'LINE_NO':item["LINE_NO"],'BUS_NO':item["BUS_NO"],"SITE_TIME":item["SITE_TIME"]},
                           {'$set':item},True)
            #src_set.update({"_id":item._id},{'$set':{"REMOVE_LABEL":1}})
            src_set.remove({"_id":ObjectId(id)})
        
        with open("dealDataInfo.log","a+") as f:
            f.write("begin time: %s,end _time:%s, deal %d items time-consuming:%f\n"%(begin_time,end_time,count,time.time()-start_time))

    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t distinctBusInfo occur exceptions:"+str(e)+"\n")

def getDataScheduleJob(mon_conn):
    mon_conn.kctest.getBusInfoJob.remove({})
    jobstores = {
    'mongo': MongoDBJobStore(collection='getBusInfoJob', database='kctest', client=mon_conn),
    'default': MemoryJobStore()
    }
    executors = {
        'default': ThreadPoolExecutor(5),
        'processpool': ProcessPoolExecutor(1)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 5    #多个实例是避免任务堵塞，如果单个实例的话，上个任务未执行完成就轮到下个任务执行，就会冲突
    }
    scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.add_job(readWriteBusRunInfo,'interval',seconds=15, jobstore='default')
    #定时任务
    scheduler.start()

def dealDataScheduleJob(mon_conn):
    #每次重启程序时需要删除队列
    mon_conn.kctest.dealDataJob.remove({})
    jobstores = {
    'mongo': MongoDBJobStore(collection='dealDataJob', database='kctest', client=mon_conn),
    'default': MemoryJobStore()
    }
    executors = {
        'default': ThreadPoolExecutor(12),
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 10    #avoid block
    }

    scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.add_job(readWriteBusRunInfo,'interval',seconds=15,jobstore="mongo")
    scheduler.add_job(distinctBusInfo,'interval',seconds=30, jobstore='mongo')
    #定时任务
    scheduler.add_job(statistics,"cron",hour=23,minute=30,jobstore='mongo')
    #scheduler.add_job(statistics,"interval",seconds=30,jobstore='default')
    scheduler.start()

def main():
    try:
        global ora_db,mon_db
        #connect oracle database
        ora_db=oracle.connect(ora_userName+"/"+ora_password+"@"+ora_ipAddr+":"+ora_port+"/"+ora_service)
        #connect mongodb database
        mon_conn=MongoClient(mon_ipAdrr,mon_port)
        mon_db=mon_conn.kctest              #create database
        #kctest_backup=mon_conn.kctest_backup

        #select bus line info from oracle database,insert into mongodb datbase 
        #readWriteLineInfo(ora_db,mon_db)
        #readWriteStationLineInfo(ora_db,mon_db)
        
        d=date.today()
        today=datetime(d.year, d.month, d.day)
        #readWriteBusRunInfo(mon_db,today)
        #distinctBusInfo()
        dealDataScheduleJob(mon_conn)
        #getDataScheduleJob(mon_conn)
        ora_db.close()
        mon_conn.close()
    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t main occur exceptions:"+str(e)+"\n")

if __name__ == "__main__":
    main()
