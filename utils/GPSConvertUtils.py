#coding=utf-8
import numpy as np
import math

"""
* 坐标转换工具类
* WGS84: Google Earth采用，Google Map中国范围外使用
* GCJ02: 火星坐标系，中国国家测绘局制定的坐标系统，由WGS84机密后的坐标。Google Map中国和搜搜地图使用，高德
* BD09:百度坐标，GCJ02机密后的坐标系
* 搜狗坐标系，图吧坐标等，估计也是在GCJ02基础上加密而成的
"""

pi = 3.1415926535897932384626
a = 6378245.0
ee = 0.00669342162296594323

#WGS1984-->GCJ02
def convert_WGS84_to_GCJ02(lng,lat):
    if outOfChina(lng,lat):
        return
    dlat=transformLat(lng-105.0,lat-35.0)
    dlng=transformLng(lng-105.0,lat-35.0)
    rlat=rad(lat)
    magic=math.sin(rlat)
    magic=1-ee*magic*magic
    smagic=math.sqrt(magic)
    dlat=(dlat * 180.0) / ((a * (1 - ee)) / (magic * smagic) * pi)
    dlng=(dlng * 180.0) / (a / smagic * math.cos(rlat) * pi)
    glat=lat+dlat
    glng=lng+dlng
    return glng,glat

#火星坐标系 (GCJ-02) 与百度坐标系 (BD-09) 的转换算法 将 GCJ-02 坐标转换成 BD-09 坐标
def convert_GCJ02_to_BD09(glng,glat):
    x=glng
    y=glat
    z=math.sqrt(x*x+y*y)+0.00002 * math.sin(y * pi)
    theta=math.atan2(y,x)+0.000003 * math.cos(x * pi)
    blng=z*math.cos(theta)+0.0065
    blat=z*math.sin(theta)+0.006
    return blng,blat

def convert_WGS84_to_BD09(lng,lat):
    glng,glat=convert_WGS84_to_GCJ02(lng,lat)
    blng,blat=convert_GCJ02_to_BD09(glng,glat)
    return blng,blat
    
def convert_GCJ02_to_WGS84(glng,glat):
    lng,lat=convert_WGS84_to_GCJ02(glng,glat)
    lng=glng*2-lng
    lat=glat*2-lat
    return lng,lat

def convert_BD09_to_WGS84(blng,blat):
    glng,glat=convert_BD09_to_GCJ02(blng,blat)
    lng,lat=convert_GCJ02_to_WGS84(glng,glat)
    return lng,lat

def convert_BD09_to_GCJ02(blng,blat):
    x = blng - 0.0065
    y = blat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * pi)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * pi)
    glng = z * math.cos(theta)
    glat = z * math.sin(theta)
    return glng,glat

def transformLat(x,y):
    ret=-100.0+2.0*x+ 3.0 * y + 0.2 * y * y + 0.1 * x * y+ 0.2 * math.sqrt(abs(x))
    ret=ret+(20.0 * math.sin(6.0 * x * pi) + 20.0 * math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret=ret+(20.0 * math.sin(y * pi) + 40.0 * math.sin(y / 3.0 * pi)) * 2.0 / 3.0
    ret=ret+(160.0 * math.sin(y / 12.0 * pi) + 320 * math.sin(y * pi / 30.0)) * 2.0 / 3.0
    return ret

def transformLng(x,y):
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1* math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * pi) + 20.0 * math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * pi) + 40.0 * math.sin(x / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * pi) + 300.0 * math.sin(x / 30.0* pi)) * 2.0 / 3.0
    return ret

def rad(d):
    return d*pi/180.0

#calculate distance between two points in WGS1984,measure:meter
def getDistanceWGS(lng1,lat1,lng2,lat2):
    r=6378137
    #print(lng1,lat1)
    #lng1,lat1=convert_WGS84_to_BD09(lng1,lat1)
    #print(lng1,lat1)
    #lng2,lat2=convert_WGS84_to_BD09(lng2,lat2)
    radLat1=rad(lat1)
    radLat2=rad(lat2)
    a=radLat1-radLat2
    b=rad(lng1)-rad(lng2)
    s=2*math.asin(math.sqrt(math.pow(math.sin(a/2),2)+math.cos(radLat1)*math.cos(radLat2)*math.pow(math.sin(b/2),2)))
    s=s*r
    s=round(s*10000)/10000
    #print(s)
    return s

def outOfChina(lon,lat):
    if lon<72.004 or lon>173.8347:
        return True
    if lat<0.8293 or lat>55.8271:
        return True
    return False

if __name__=="__main__":
    #119.298314,26.000763
    lng2,lat2=convert_BD09_to_WGS84(119.298314,26.000763)
    lng1,lat1=convert_WGS84_to_BD09(119.287683,26.001150)
    #lng2,lat2=convert_WGS84_to_GCJ02(119.286000,26.024917)
    print(lng1,lat1)
    print(lng2,lat2)
    print(getDistanceWGS(119.287683,26.001150,lng2,lat2))