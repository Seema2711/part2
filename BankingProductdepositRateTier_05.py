# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 21:39:26 2020

@author: palas
"""

from class_Connector import Connector
from class_Transform import Transform
import pandas as pd

class BankingProductdepositRateTier():
    
    def __init__(self, root, RunByUsername, log):
        
        log.logComment("BankingProductdepositRateTier (05) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        self.FailInd = conn.openConnector()
        
        transform = Transform(conn, root, log)
                
        transform.fileList = ['RatesFile.csv']
        
        
        # parentTable = "select productid, depositRateId, applicationfrequency, depositratetype, rate from bankingproductdepositrate".lower()
        parentTable = "select productid, depositrateid, applicationfrequency, depositratetype, trim(replace(cast(rate as char), '0','')) from bankingproductdepositrate".lower()
        parentdata = pd.DataFrame(conn.executeQuery(parentTable))
        
        preLoad = ('select depositRateId, name, unitOfMeasure, minimumValue, maximumValue,'
                      'rateApplicationMethod from bankingproductdepositRateTier').lower()
        preLoadData = conn.executeQuery(preLoad)
        
        preLoadDataList = []
        if len(preLoadData) != 0:
            for row in preLoadData:
                row = [str(x) for x in row]
                preLoadDataList.append('|'.join(row))
        
        
        extractDataTS = transform.getDF('TnS_Rate.csv')
        
        extractDataTS = pd.merge(parentdata, extractDataTS, how = 'left', left_on = [0, 2, 3, 4], right_on = ['Product ID (Mandatory)','applicationFrequency (Auto Generated)', 'Rate Type (Mandatory)', 'Rate (Mandatory)'])
        extractDataTS = extractDataTS.dropna(subset = ['Rate Type (Mandatory)'])
        df_cols = [1, 'Name', 'Unit of Measure', 'Minimum Value', 'Maximum Value', 'Rate Application Method']
        extractDataTS = extractDataTS[df_cols]
        
        
        extractDataRF = transform.getDF('RatesFile.csv')
        
        if not extractDataRF.empty:
            extractDataRF = pd.merge(parentdata, extractDataRF, how = 'left', left_on = [0, 2, 3], right_on = ['Product ID', 'Additional Value (Auto Generated)','Rate Type (Mandatory)'])
            extractDataRF = extractDataRF.dropna(subset = ['Rate Type (Mandatory)'])
            
            df_cols = [1, 'Name', 'Unit of Measure', 'minimumValue', 'maximumValue', 'Rate Application Method']
            extractDataRF = extractDataRF[df_cols]
            extractDataTS.columns = df_cols

        extractData = pd.concat([extractDataTS, extractDataRF])
        
        
        extractdataOutput = []
        for i in range(len(extractData)):
            dat = extractData.iloc[[i],:].values.tolist()[0]
            dat = [str(x) for x in dat]
            extractdataOutput.append('|'.join(dat))
            
        insertSelection = []
        
        for row in extractdataOutput:
            if row not in preLoadDataList:
                insertSelection.append(row)
        
        seqQuery = 'Select max(cast(depositRateTierId as decimal)) from bankingproductdepositRateTier'.lower()
        seq = conn.executeQuery(seqQuery)
        seq = seq[0][0]
        if seq == None:
            seq = 0
        else:
            seq = int(seq)
        
        insertQuery = ('insert into bankingproductdepositRateTier '
                       '(depositRateTierId, depositRateId, name, unitOfMeasure, minimumValue, '
                       'maximumValue, rateApplicationMethod, '
                       'createdOn, createdBy, systemCreatedOn, systemCreatedBy) '
                       'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                ).lower()
        
        
        reject = []
        
        for row in insertSelection:
            seq += 1
            d = "|".join([str(x) for x in [seq, row, conn.updatedDate, conn.createdBy, conn.updatedDate, conn.createdBy]])
            d = d.replace("|False|","|0|")
            d = d.replace("|nan","|")
            d = d.replace("|None","|")
            d = d.split("|")
            if d[4] == '':
                d[4] = '0'
            if d[5] == '':
                d[5] = '0'
                
            if conn.insertSingleRow(insertQuery,d) == 1:
                reject.append(d)
        log.logComment(reject)
        conn.closeConnector()
        
