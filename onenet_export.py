#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 11:12:07 2019

@author: Daniel Lou
"""

import requests
import json
import time
import openpyxl
import os
import datetime
 
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

def readExcel(ExelFilename):
        filename = ExelFilename
        inwb = openpyxl.load_workbook(filename)  # 读文件

        sheetnames = inwb.get_sheet_names()  # 获取读文件中所有的sheet，通过名字的方式
        ws = inwb.get_sheet_by_name(sheetnames[0])  # 获取第一个sheet内容

        # 获取sheet的最大行数和列数
        rows = ws.max_row
        cols = ws.max_column
        for r in range(1,rows):
            for c in range(1,cols):
                print(ws.cell(r,c).value)
            if r==10:
                break

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
    outws.cell(1, 10).value = 'PCI'
    outws.cell(1, 11).value = 'ECL'
    outws.cell(1, 12).value = 'BatPer'
    outws.cell(1, 13).value = 'ICCID'
    outws.cell(1, 14).value = 'CellID'
    
    outws.cell(1, 15).value = 'value'
    
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

def writeStream2excel(APIKEY,DeviceID,StreamName,Outws,Row_start):
    url = 'http://api.heclouds.com/devices/%s/datapoints?limit=3000'%DeviceID
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
        outws.cell(row_start + row, 3).value = datapoints[row]['at']  # timestamp
        lv = datapoints[row]['value']
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
            
        outws.cell(row_start + row, 2).value = s[4:20]  # imei
        if row < (len(datapoints) -1): 
            dd = datetime.datetime.strptime(datapoints[row]['at'][:19],'%Y-%m-%d %H:%M:%S')- datetime.datetime.strptime(i['datapoints'][row+1]['at'][:19],'%Y-%m-%d %H:%M:%S') 
            td = round(dd.seconds/3600.0,1)
            outws.cell(row_start + row, 4).value = int(td)
        else:             
            outws.cell(row_start + row, 4).value = 0
            
        if s[2:4] == 'd9':    
            outws.cell(row_start + row, 5).value = hexstr2int(s[20:22])  # CSQ
            outws.cell(row_start + row, 6).value = hexstr2int(s[34:36])  # 'Temperature'
            outws.cell(row_start + row, 7).value = hexstr2int(s[36:40])/100.00  # 'Voltage'
            #print('imei: ', s[4:20])
            #print('csq: ', s[20:22], hexstr2int(s[20:22]))
            #print ('temperature:', s[34:36], hexstr2int(s[34:36]))
            #print('val:', s)
            #print ('rsrp:', s[40:44])
            outws.cell(row_start + row, 8).value = hexstr2int(s[40:44])/10  # 'RSRP'
            outws.cell(row_start + row, 9).value = hexstr2int(s[44:48])/10  # 'SINR'
            outws.cell(row_start + row, 10).value = hexstr2int(s[48:52])  # 'PCI'
            outws.cell(row_start + row, 11).value = hexstr2int(s[52:54])  # 'ECL'
            outws.cell(row_start + row, 12).value = hexstr2int(s[54:56])  # 'BatPer'
            outws.cell(row_start + row, 13).value = s[56:76]  # 'ICCID'
            outws.cell(row_start + row, 14).value = hexstr2int(s[76:80])  # 'CellID'
        
        outws.cell(row_start + row, 15).value = s #value string in format 0x...
        #print(s)
    row_start = row_start + len(datapoints)
    return row_start
        
            
if __name__ == "__main__":
    #DeviceID = '512447860'                   #设备ID
    ApiKey = 'az9mLpzgGdnRT8iDQqACTXTACnM=' #APIKey管理中的默认APIKEY
    DataStreamName = '3200_0_5505'          #数据流名称，没有则新建数据流
    #device = Device(DeviceID,ApiKey)
    #device.upload_point(DataStreamName,66)  #向数据流中添加新数据
    #print(device.get_stream(DataStreamName)) #查询数据流中的最新数据
    product = Product(ApiKey)
    devices = product.get_devices()
    filepath = os.getcwd() + '/devices.xlsx'
    writeDevices2Excel(devices,filepath)
    writeStreams2Excel(ApiKey,devices,DataStreamName,filepath)
                       
