#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 14:44:55 2019

@author: daniel Lou
"""

import requests
import json
import time
import openpyxl
import os
import datetime
import pymysql
import traceback
import threading

datapoint_count = 0
threadLock = threading.Lock()

class ExportThread (threading.Thread):
#    def __init__(self, threadID''', APIKEY, Devices, StreamName, Start, TableName'''):
    def __init__(self, threadID, threadName, APIKEY, Devices, StreamName, StartTime, TableName):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.apikey = APIKEY
        self.devices = Devices
        self.streamname = StreamName
        self.starttime = StartTime
        self.tablename = TableName
    def run(self):
        #print ("开始线程：%s"%(self.threadName))
        WriteData2DB(self.apikey, self.devices, self.streamname, 
                     self.starttime, self.tablename)
        #print ("退出线程：%s"%(self.threadName))

    def getThreadName(self):
        return self.threadName


def WriteDevices2DB(Devices, TableName):
    # 打开数据库连接
    db = pymysql.connect("localhost","root","root.1234","iot" )
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    cursor.execute('TRUNCATE TABLE %s'%TableName)
    
    devices = Devices
    sub_sql1 = 'INSERT INTO %s('%TableName
    keys = list(set(devices[0].keys()))
    keys.sort()
    sub_sql2= ''
    for k in keys:
        sub_sql1 += '`%s`,'%k
    sub_sql1 = sub_sql1[:len(sub_sql1)-1] + ')'
    for d in devices:
        sub_sql2 = ' values('
        for k in keys:
            sub_sql2 += '\"%s\",'%d.get(k)
        sub_sql2 = sub_sql2[:len(sub_sql2)-1]
        sub_sql2 += ')'
        
    
        try:
            # 执行sql语句
            cursor.execute(sub_sql1 + sub_sql2)
            # 提交到数据库执行
            db.commit()
        except:
            #输出异常信息  
            traceback.print_exc()
            print(sub_sql1 + sub_sql2)
            db.rollback()
    
    cursor.close()
    db.close()
    print('No:%d devices save to mysql db'%len(devices))

def WriteData2DBMT(DevicesPerThread, APIKEY, Devices, StreamName, StartTime, TableName):
    if len(Devices) == 0:
        return []
    i = 0
    threads =[]
    for i in range(len(Devices)//DevicesPerThread):
        devices = Devices[i*DevicesPerThread:(i+1)*DevicesPerThread]
        thread = ExportThread(i+1, 'thread-%s-%d'%(TableName, (i+1)), APIKEY, devices, StreamName, StartTime, TableName)
        threads.append(thread)
    if len(Devices) < DevicesPerThread:
        devices = Devices
    else:
        devices = Devices[(i+1)*DevicesPerThread:]
        i += 1
    thread = ExportThread(i+1, 'thread-%s-%d'%(TableName, (i+1)), APIKEY, devices, StreamName, StartTime, TableName)
    threads.append(thread)
    for t in threads:
        t.start()
    return threads
    


def WriteData2DB(APIKEY, Devices, StreamName, StartTime, TableName):
    global datapoint_count
    devices = Devices
    strStart = StartTime
    for d in devices:
        url = 'http://api.heclouds.com/devices/%s/datapoints?sort=DESC&limit=1000&start=%s'%(d['id'],strStart)
        html = getUrlRespHtml(APIKEY, url)
        if html.find('fail') >= 0:
            continue
        data = json.loads(html)
        ds = []

        while True:
            if data['data']['count'] > 0 :
                for i in data['data']['datastreams']:
                    if i['id'] == StreamName:
                        ds.extend(i['datapoints'])
            if 'cursor' in data['data'].keys():
                url1 = '&cursor=%s'%data['data']['cursor']
                html = getUrlRespHtml(APIKEY, url + url1)
                print(url+url1)
                data = json.loads(html)
                #print(url)
            else:
                break
        
        if len(ds) > 0:
            WriteData2DBbyDevice(d['id'], ds, TableName)
        
        # 获取锁，用于线程同步
        threadLock.acquire()
        datapoint_count += len(ds)
        # 释放锁，开启下一个线程
        threadLock.release()
        #print('No:%d device id:%s datapoint number:%d save to mysql db'%(devices.index(d) + 1, d['id'], len(ds)))
        #if devices.index(d)%50 == 0:
        #    time.sleep(10)
        
def WriteData2DBbyDevice(DeviceID, DataStream, TableName):
    if len(DataStream) == 0:
        return
    data = DataStream
    '''cols = ['deviceid','imei','time','period','CSQ','Temperature','Voltage',
            'RSRP','SINR','last_CSQ','last_Temperature','last_Voltage',
            'last_RSRP','last_SINR','PCI','ECL','BatPer','ICCID','CellID',
            'FrameType','value']'''

    # 打开数据库连接
    db = pymysql.connect("localhost","root","root.1234","iot" )
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    for i in range(len(data)):
        last_s = '0xd9'+ '00'*50
        s = list2hexstr(data[i]['value'])
        
        deviceid = DeviceID
        imei = s[4:20]  # imei
        time = data[i]['at'][:19]
        if i < (len(data) -1): 
            dd = datetime.datetime.strptime(data[i]['at'][:19],'%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(data[i+1]['at'][:19],'%Y-%m-%d %H:%M:%S')
            td = round(dd.seconds/3600.0,1)
            last_s = list2hexstr(data[i+1]['value'])
            period = str(int(td))
#            print('t1=%s t2=%s td=%d period=%s'%(data[i]['at'][:19],data[i+1]['at'][:19],int(td),period))
        else:             
            period = str(0)
        
        FrameType = s[2:4]
        
        CSQ = ''  # CSQ
        Temperature = '' # 'Temperature'
        Voltage = ''  # 'Voltage'
        RSRP = ''  # 'RSRP'
        SINR = ''  # 'SINR'
        last_CSQ = ''  # last-CSQ
        last_Temperature = ''  # last-Temperature'
        last_Voltage = ''  # last Voltage'
        last_RSRP = ''  # last RSRP'
        last_SINR = ''  # last SINR'
        PCI = '' # 'PCI'
        ECL = ''  # 'ECL'
        BatPer = '' # 'BatPer'
        ICCID = ''  # 'ICCID'
        CellID = ''  # 'CellID'
        if FrameType == 'd9':    
            CSQ = hexstr2int(s[20:22])  # CSQ
            Temperature = hexstr2int(s[34:36])  # 'Temperature'
            Voltage = hexstr2int(s[36:40])/100.00  # 'Voltage'
            RSRP = hexstr2int(s[40:44])/10  # 'RSRP'
            SINR = hexstr2int(s[44:48])/10  # 'SINR'
            
            if last_s[2:4] == 'd9':
                last_CSQ = hexstr2int(last_s[20:22])  # last-CSQ
                last_Temperature = hexstr2int(last_s[34:36])  # last-Temperature'
                last_Voltage = hexstr2int(last_s[36:40])/100.00  # last Voltage'
                last_RSRP = hexstr2int(last_s[40:44])/10  # last RSRP'
                last_SINR = hexstr2int(last_s[44:48])/10  # last SINR'

            PCI = hexstr2int(s[48:52])  # 'PCI'
            ECL = hexstr2int(s[52:54])  # 'ECL'
            BatPer = hexstr2int(s[54:56])  # 'BatPer'
            ICCID = s[56:76]  # 'ICCID'
            CellID = hexstr2int(s[76:80])  # 'CellID'
        
        value = s #value string in format 0x...
        
        sql = 'INSERT INTO %s values(\'%s\',\'%s\',\'%s\',%s,\'%s\',\
        \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\
        \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')'%(TableName,deviceid,imei,
        time,period,CSQ,Temperature,Voltage,RSRP,SINR,last_CSQ,last_Temperature,
        last_Voltage,last_RSRP,last_SINR,PCI,ECL,BatPer,ICCID,CellID,FrameType,value)
    
    
        try:
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            db.commit()
        except:
            #输出异常信息  
            traceback.print_exc()  
            print(sql)
            db.rollback()
    
    cursor.close()
    db.close()

def getUrlRespHtml(APIKEY, url):
    heads = { "api-key":APIKEY,
             "Connection":"close"}
    try:
        respHtml = requests.get(url,headers=heads,timeout=60).text
    except:
        print("Fail to open URL:" + url)
        return 'fail'
    return respHtml

def getDevices(APIKEY):
    url='http://api.heclouds.com/devices'
    html = getUrlRespHtml(APIKEY, url)
    if html.find('fail') >= 0:
        return []
    data = json.loads(html)
    devices = data['data']['devices']
    total_count = data['data']['total_count']
    per_page = data['data']['per_page']
    
    for p in range(2,total_count//per_page + 2):
        url='http://api.heclouds.com/devices'+ '?page=%d'%p
        #print(url)
        html = getUrlRespHtml(APIKEY, url)
        if html.find('fail') >= 0:
            continue
        data = json.loads(html)
        devices.extend(data['data']['devices'])
    return devices

def hexstr2int(HexStr):
    val = int(HexStr, 16)
    if len(HexStr) == 2:
        if val&0x80 > 0:
            val = val -1
            val = val^0xff
            val = 0 - val
            return val
    if len(HexStr) == 4:
        val = int(HexStr, 16)
        if val&0x8000 > 0:
            val = val -1
            val = val^0xffff
            val = 0 - val
    return val

def list2hexstr(lv):
    s = '0x'
    for l in lv:
        if l < 0:
            strh = hex(((0-l)^0xff) + 1)
            strh = strh[2:]
            strh = strh.zfill(2)
        else:
            strh = hex(l)
            strh = strh[2:]
            strh = strh.zfill(2)
        s += strh
    return s

if __name__ == "__main__":
    #DeviceID = '512447860'                   #设备ID
    APIKEY_HKRM_SD = 'az9mLpzgGdnRT8iDQqACTXTACnM=' #APIKey管理中的默认APIKEY
    APIKEY_HKBL_SD = 'vIYkqpTEQEgtUvnyqIZsjauvgqM=' #APIKey管理中的默认APIKEY
    TBL_HKRM_DEVICES = 'sd_devices_hkrm'
    TBL_HKBL_DEVICES = 'sd_devices_hkbl'
    TBL_HKRM_DATA = 'sd_data_hkrm'
    TBL_HKBL_DATA = 'sd_data_hkbl'
    DataStreamName = '3200_0_5505'          #数据流名称，没有则新建数据流
    DEVICES_PER_THREAD = 50                  #devices number per thread
    #device = Device(DeviceID,ApiKey)
    #device.upload_point(DataStreamName,66)  #向数据流中添加新数据
    #print(device.get_stream(DataStreamName)) #查询数据流中的最新数据
    '''product = Product(ApiKey)
    devices = product.get_devices()
    filepath = os.getcwd() + '/devices.xlsx'
    writeDevices2Excel(devices,filepath)
    writeStreams2Excel(ApiKey,devices,DataStreamName,filepath)'''
    while True:
        t1=time.time()

        dtStart = datetime.datetime.now()-datetime.timedelta(hours=1)
        strStart = datetime.datetime.strftime(dtStart, '%Y-%m-%dT%H:%M:%S')

        threads = []
        devices = getDevices(APIKEY_HKBL_SD)
        WriteDevices2DB(devices, TBL_HKBL_DEVICES)
        threads.extend(WriteData2DBMT(DEVICES_PER_THREAD,APIKEY_HKBL_SD,
                                      devices,DataStreamName,
                                      strStart,TBL_HKBL_DATA))

        devices = getDevices(APIKEY_HKRM_SD)
        WriteDevices2DB(devices, TBL_HKRM_DEVICES)
        threads.extend(WriteData2DBMT(DEVICES_PER_THREAD,APIKEY_HKRM_SD,
                                      devices,DataStreamName,
                                      strStart, TBL_HKRM_DATA))
        for t in threads:
            t.join()
        
        t2=time.time()
        t_now = datetime.datetime.now()
        print ('[%s]: 单次下载用时%s s, %d datapoints export into mysql'%(
                datetime.datetime.strftime(t_now, '%Y-%m-%d %H:%M:%S'), 
                int(t2-t1), datapoint_count))
        
        # 获取锁，用于线程同步
        threadLock.acquire()
        datapoint_count = 0
        # 释放锁，开启下一个线程
        threadLock.release()

        time.sleep(3600-t2+t1)

