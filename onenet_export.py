#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 11:12:07 2019

@author: shdt
"""

import requests
import json
import time
import openpyxl
import os
import datetime
import pymysql
import traceback


class Device():
    def __init__(self,DEVICEID,APIKEY):
        self.DEVICEID = DEVICEID
        self.APIKEY = APIKEY
        self.url = 'http://api.heclouds.com/devices/%s/datapoints'%(self.DEVICEID)
        self.headers = { "api-key":self.APIKEY,"Connection":"close"}
    def upload_point(self,DataStreamName,VALUE):
        dict = {"datastreams":[{"id":"id","datapoints":[{"value":0}]}]}
        dict['datastreams'][0]['id'] = DataStreamName
        dict['datastreams'][0]['datapoints'][0]['value'] = VALUE
        if "succ" in requests.post(self.url,headers=self.headers,data = json.dumps(dict)).text:
            print("Value:",VALUE," has been uploaded to ",DataStreamName," at ",time.ctime())
    def get_stream(self,DataStreamName):
        data = json.loads(requests.get(self.url,headers=self.headers,).text)
        for i in data['data']['datastreams']:
            if i["id"] == DataStreamName:
                return i['datapoints']
        else:
            return "Not found DataStreamName - %s "%DataStreamName
        

class Product():
    def __init__(self,APIKEY):
        self.APIKEY = APIKEY
        self.url = 'http://api.heclouds.com/devices'
        self.headers = { "api-key":self.APIKEY,"Connection":"close"}
    def get_devices(self):
        data = json.loads(requests.get(self.url,headers=self.headers,).text)
        devices = data['data']['devices']
        total_count = data['data']['total_count']
        per_page = data['data']['per_page']

        for p in range(2,total_count//per_page + 2):
            data = json.loads(requests.get(self.url + '?page=%d'%p ,headers=self.headers,).text)
            devices.extend(data['data']['devices'])
            
        return devices


def getUrlRespHtml(APIKEY, url):
    heads = { "api-key":APIKEY,
             "Connection":"close"}
    try:
        respHtml = requests.get(url,headers=heads,timeout=10).text
    except [requests.exceptions.HTTPError, 
            requests.exceptions.ConnectTimeout, 
            requests.exceptions.ConnectionError] as e:
        print(e.code)
        print("Fail to open URL:" + url)
        return 'fail'
    return respHtml

def getDevices(APIKEY):
    url='http://api.heclouds.com/devices'
    html = getUrlRespHtml(APIKEY, url)
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

def TruncTable(Table):
    # 打开数据库连接
    db = pymysql.connect("localhost","root","root.1234","iot" )
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    cursor.execute('TRUNCATE TABLE %s'%Table)
    # 提交到数据库执行
    db.commit()
    cursor.close()
    db.close()
    


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

def WriteData2DB(APIKEY, Devices, StreamName, Start, TableName):
    devices = Devices
    start = datetime.datetime.strftime(Start, '%Y-%m-%dT%H:%M:%S')
    for d in devices:
        url = 'http://api.heclouds.com/devices/%s/datapoints?sort=DESC&limit=1000&start=%s'%(d['id'],start)
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
        print('No:%d device id:%s datapoint number:%d save to mysql db'%(devices.index(d) + 1, d['id'], len(ds)))
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

def writeDevices2Excel(Devices,ExcelFilename):
    outwb = openpyxl.Workbook()  # 打开一个将写的文件
    outws = outwb.create_sheet(index=0)  # 在将写的文件创建sheet
    devices = Devices
    #write colum header
    keys = devices[0].keys()
    for i in range(1,len(devices)):
        keys=list(set(keys).union(set(devices[i].keys())))

    keys.sort()
    
    maxcol = len(keys)
    for col in range(1,maxcol):
        outws.cell(1, col).value = keys[col-1]

    col = 1       
    for row in range(2,len(devices)+2):
        for col in range(1,maxcol):
            value = devices[row-2].get(keys[col-1])
            if type(value) == dict:
                value = str(value)
            if type(value) == list:
                value = str(value)
            #print value
            outws.cell(row, col).value = value  # 写文件
    saveExcel = ExcelFilename
    outwb.save(saveExcel)  # 一定要记得保存
    #print "device count:",len(devices)


def writeStreams2Excel(APIKEY,Devices,StreamName,ExcelFilename):
    outwb = openpyxl.load_workbook(ExcelFilename)  # 打开一个将写的文件
    outws = outwb.create_sheet(index=0)  # 在将写的文件创建stream sheet
    devices = Devices
    #write colum header
    outws.cell(1, 1).value = 'deviceid'
    outws.cell(1, 2).value = 'imei'
    outws.cell(1, 3).value = 'time'
    outws.cell(1, 4).value = 'period'
    outws.cell(1, 5).value = 'CSQ'
    outws.cell(1, 6).value = 'Temperature'
    outws.cell(1, 7).value = 'Voltage'
    outws.cell(1, 8).value = 'RSRP'
    outws.cell(1, 9).value = 'SINR'

    outws.cell(1, 10).value = 'last-CSQ'
    outws.cell(1, 11).value = 'last-Temperature'
    outws.cell(1, 12).value = 'last-Voltage'
    outws.cell(1, 13).value = 'last-RSRP'
    outws.cell(1, 14).value = 'last-SINR'

    outws.cell(1, 15).value = 'PCI'
    outws.cell(1, 16).value = 'ECL'
    outws.cell(1, 17).value = 'BatPer'
    outws.cell(1, 18).value = 'ICCID'
    outws.cell(1, 19).value = 'CellID'
    
    outws.cell(1, 20).value = 'value'
    
    row_start = 2
    for d in devices:
        row_start = writeStream2excel(APIKEY,d['id'],StreamName,outws,row_start)
        print('device number:%d device id:%s download into excel'%(devices.index(d)+1,d['id']))
        outwb.save(ExcelFilename)  # 一定要记得保存
    outwb.save(ExcelFilename)  # 一定要记得保存

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

def writeStream2excel(APIKEY,DeviceID,StreamName,Outws,Row_start):
    url = 'http://api.heclouds.com/devices/%s/datapoints?limit=6000'%DeviceID
    httphd = { "api-key":APIKEY,"Connection":"close"}
    row_start = Row_start
    outws = Outws
    datapoints = []
    while True:
        data = json.loads(requests.get(url,headers=httphd,).text)
        if data['data']['count'] > 0 :
            for i in data['data']['datastreams']:
                if i['id'] == StreamName:
                    datapoints.extend(i['datapoints'])
        if 'cursor' in data['data'].keys():
            url = url + '&cursor=%s'%data['data']['cursor']
            print(url)
        else:
            break
    print('datapoints len:', len(datapoints))
    for row in range(len(datapoints)):
        outws.cell(row_start + row, 1).value = DeviceID  # 写文件
        #print(datapoints[row])
        outws.cell(row_start + row, 3).value = datapoints[row]['at'][:19]  # timestamp
        lv = datapoints[row]['value']
        last_s = '0xd9'+ '00'*50
        s = list2hexstr(lv)
            
        outws.cell(row_start + row, 2).value = s[4:20]  # imei
        if row < (len(datapoints) -1): 
            dd = datetime.datetime.strptime(datapoints[row]['at'][:19],'%Y-%m-%d %H:%M:%S')- datetime.datetime.strptime(datapoints[row+1]['at'][:19],'%Y-%m-%d %H:%M:%S') 
            td = round(dd.seconds/3600.0,1)
            last_s = list2hexstr(datapoints[row+1]['value'])
            outws.cell(row_start + row, 4).value = int(td)
        else:             
            outws.cell(row_start + row, 4).value = 0
            
        if s[2:4] == 'd9':    
            outws.cell(row_start + row, 5).value = hexstr2int(s[20:22])  # CSQ
            outws.cell(row_start + row, 6).value = hexstr2int(s[34:36])  # 'Temperature'
            outws.cell(row_start + row, 7).value = hexstr2int(s[36:40])/100.00  # 'Voltage'
            outws.cell(row_start + row, 8).value = hexstr2int(s[40:44])/10  # 'RSRP'
            outws.cell(row_start + row, 9).value = hexstr2int(s[44:48])/10  # 'SINR'
            
            if last_s[2:4] == 'd9':
                outws.cell(row_start + row, 10).value = hexstr2int(last_s[20:22])  # last-CSQ
                outws.cell(row_start + row, 11).value = hexstr2int(last_s[34:36])  # last-Temperature'
                outws.cell(row_start + row, 12).value = hexstr2int(last_s[36:40])/100.00  # last Voltage'
                outws.cell(row_start + row, 13).value = hexstr2int(last_s[40:44])/10  # last RSRP'
                outws.cell(row_start + row, 14).value = hexstr2int(last_s[44:48])/10  # last SINR'

            outws.cell(row_start + row, 15).value = hexstr2int(s[48:52])  # 'PCI'
            outws.cell(row_start + row, 16).value = hexstr2int(s[52:54])  # 'ECL'
            outws.cell(row_start + row, 17).value = hexstr2int(s[54:56])  # 'BatPer'
            outws.cell(row_start + row, 18).value = s[56:76]  # 'ICCID'
            outws.cell(row_start + row, 19).value = hexstr2int(s[76:80])  # 'CellID'
        
        outws.cell(row_start + row, 20).value = s #value string in format 0x...
        #end for row
    row_start = row_start + len(datapoints)
    return row_start
        
            
if __name__ == "__main__":
    #DeviceID = '512447860'                   #设备ID
    APIKEY_HKRM_SD = 'az9mLpzgGdnRT8iDQqACTXTACnM=' #APIKey管理中的默认APIKEY
    APIKEY_HKBL_SD = 'vIYkqpTEQEgtUvnyqIZsjauvgqM=' #APIKey管理中的默认APIKEY
    TBL_HKRM_DEVICES = 'sd_devices_hkrm'
    TBL_HKBL_DEVICES = 'sd_devices_hkbl'
    TBL_HKRM_DATA = 'sd_data_hkrm'
    TBL_HKBL_DATA = 'sd_data_hkbl'
    DataStreamName = '3200_0_5505'          #数据流名称，没有则新建数据流
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

        devices = getDevices(APIKEY_HKBL_SD)
        WriteDevices2DB(devices, TBL_HKBL_DEVICES)
        start = datetime.datetime.now()-datetime.timedelta(hours=1)
        WriteData2DB(APIKEY_HKBL_SD,devices,DataStreamName,start,TBL_HKBL_DATA)

        devices = getDevices(APIKEY_HKRM_SD)
        WriteDevices2DB(devices, TBL_HKRM_DEVICES)
        start = datetime.datetime.now()-datetime.timedelta(hours=1)
        WriteData2DB(APIKEY_HKRM_SD,devices,DataStreamName,start, TBL_HKRM_DATA)

        t2=time.time()
        t_now = datetime.datetime.now()
        print ('[%s]: 单次下载用时%s s'%(datetime.datetime.strftime(t_now, '%Y-%m-%d %H:%M:%S'), int(t2-t1)))
        time.sleep(3600-t2+t1)

