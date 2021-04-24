#coding=utf-8

from datetime import datetime

#把调休的休息日加到这里面
rest_holiday=[
    '2019-01-01','2019-02-04','2019-02-05','2019-02-06','2019-02-07','2019-02-08',
    '2019-04-05','2019-04-29','2019-04-30','2019-05-01','2019-06-07',
    '2019-09-13',
    '2019-10-01','2019-10-02','2019-10-03','2019-10-04','2019-10-07','2019-12-30',
    '2019-12-31',
    '2020-01-01','2020-01-24','2020-01-27','2020-01-28','2020-01-29','2020-01-30',
    '2020-04-06','2020-05-01','2020-05-04','2020-05-05','2020-06-25','2020-06-26','2020-10-01','2020-10-02',
    '2020-10-05','2020-10-06','2020-10-07','2020-10-08',
    '2021-01-01',
]

#把调休的工作日加到这里面
rest_workday=[
    '2019-02-02','2019-02-03','2019-04-27','2019-02-28','2019-09-29','2019-10-12',
    '2019-12-28','2019-12-29',
    '2020-01-19','2020-02-01','2020-04-26','2020-05-09','2020-06-28','2020-09-27','2020-10-10',
]

start_day=datetime.strptime("2019-07-01","%Y-%m-%d")
end_day=datetime.strptime("2021-12-31","%Y-%m-%d")

def getHoliday(date):
    """
     input:
        date: "%Y/%m/%d %H:%M:%S" or "%Y/%m/%d"
     return 1 represent holiday,0 workday,-1 out of range
    """
    if len(date)>10:
        date=datetime.strptime(date,"%Y/%m/%d %H:%M:%S")
    else:
        date=datetime.strptime(date,"%Y/%m/%d")
    date_str=date.strftime("%Y-%m-%d")
    
    if date>end_day or date<start_day:
        return -1
    elif date_str in rest_holiday:
        return 1
    elif date_str in rest_workday:
        return 0
    elif date.weekday() in (5,6):
        return 1
    else:
        return 0




