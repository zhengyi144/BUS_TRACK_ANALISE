from DBUtils.PooledDB import PooledDB
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

#oracle database info
ora_ipAddr="119.23.18.104"
ora_userName="gjy"
ora_password="gjy"
ora_port="1521"
ora_service="orcl"


#db=cx_Oracle.connect('gjy','gjy',cx_Oracle.makedsn("119.23.18.104", "1521", "orcl"))

#mongodb database info
mon_ipAdrr="10.10.116.200"
mon_port=27017

class OraclePool(object):
    def __init__(self):
        """
        mincached：       初始化时，链接池中至少创建的空闲的链接，0表示不创建
        maxcached：       连接池最大闲置的可用连接数量
        maxshared：       连接池最大可共享连接数量
        maxconnections：  最大允许连接数量，0和None表示不限制
        blocking：        连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
        maxusage：        单个连接最大复用次数
        """
        dsn = oracle.makedsn(ora_ipAddr, ora_port, service_name=ora_service)
        self.pool = PooledDB(oracle, user=ora_userName, password=ora_password, dsn=dsn, mincached=2,maxcached=2, maxconnections=3,blocking=True)
        
    def execute_sql(self,sql,args):
        """
          sql: "select LINE_NO from kctest_bus_run_info where site_time>=:today"
          args: {"today":today}

          note:
             ！！！使用这个函数后一定要关闭游标
        """
        result=()
        conn=self.pool.connection()
        cur=conn.cursor()
        try:
            if args:
                result=cur.execute(sql,args).fetchall()
            else:
                result=cur.execute(sql).fetchall()
        except Exception as e:
            with open("exception.log","a+") as f:
                f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t execute_sql  exceptions:"+str(e)+"\n")
        cur.close()
        conn.close()
        return result

    def fetch_all(self,sql,args=None):
        dataset=self.execute_sql(sql,args)
        return dataset

def readWriteBusRunInfo():
    hour=datetime.now().hour
    if hour<6 or hour>23:
        return
    try:
        start_time=time.time()
        d=date.today()
        today=datetime(d.year, d.month, d.day)

        dataset=ora_pool.fetch_all("select LINE_NO,BUS_NO,BUS_NO_CHAR,IS_UP_DOWN,LNG,LAT,SITE_TIME,RUN_STATE,WILL_RUN from kctest_bus_run_info where is_up_down in (1,0) and run_state=1 and site_time>=:today",{"today":today})
        #dataset=cursor.fetchmany(2)
        
        #create mongodb table
        mon_set=mon_db.kctest_bus_run_info       
        #测试公交到站时间
        mon_set.remove({})
        items=[]
        for data in dataset:
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
        mon_set.insert_many(items)
        #print(items[0])
        
        with open("readDataInfo.log","a+") as f:
            f.write("local time:%s, insert %d bus info time-consuming:%f\n"%(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()),len(items),time.time()-start_time))

    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t readWriteBusRunInfo occur exceptions:"+str(e)+"\n")

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

def getDataScheduleJob(mon_conn=None):
    #mon_conn.kctest.getBusInfoJob.remove({})
    jobstores = {
    #'mongo': MongoDBJobStore(collection='getBusInfoJob', database='kctest', client=mon_conn),
    'default': MemoryJobStore()
    }
    executors = {
        'default': ThreadPoolExecutor(5),
        'processpool': ProcessPoolExecutor(1)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3    #多个实例是避免任务堵塞，如果单个实例的话，上个任务未执行完成就轮到下个任务执行，就会冲突
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
        'default': ThreadPoolExecutor(4),
        'processpool': ProcessPoolExecutor(1)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3    #avoid block
    }

    scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.add_job(readWriteBusRunInfo,'interval',seconds=15,jobstore="mongo")
    #scheduler.add_job(distinctBusInfo,'interval',seconds=30, jobstore='mongo')
    #定时任务
    #scheduler.add_job(statistics,"cron",hour=23,minute=59,jobstore='mongo')
    #scheduler.add_job(statistics,"interval",seconds=30,jobstore='default')
    scheduler.start()

def main():
    try:
        global ora_pool,mon_db
        ora_pool=OraclePool()
        #connect mongodb database
        mon_conn=MongoClient(mon_ipAdrr,mon_port)
        mon_db=mon_conn.kctest              #create database
        #getDataScheduleJob(mon_conn)
        dealDataScheduleJob(mon_conn)
        mon_conn.close()
    except Exception as e:
        with open("exception.log","a+") as f:
            f.write("local time:"+time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())+"\t main occur exceptions:"+str(e)+"\n")

if __name__=="__main__":
    main()