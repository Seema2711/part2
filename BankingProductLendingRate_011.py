# -*- coding: utf-8 -*-
"""
Created on Wed May 27 10:51:12 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class BankingProductLendingRate():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("BankingProductLendingRate (011) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("BankingProductLendingRate (011) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Rate.csv']
        
        transform.df_cols = ['Product ID (Mandatory)', 'CUA Effective From (Mandatory) Date/Time',
                               'Rate Type (Mandatory)', 'Rate (Mandatory)', 'Comparison Rate', 
                               'calculationFrequency (Auto Generated)', 'Additional Value (Auto Generated)',
                               'Additional Details', 'Additional Information', 'Additional Information URL'
                              ]
        
        transform.joinerColumn = 'Rate Type (Mandatory)'
        
        tableQuery = ("SELECT Productid, lastUpdated, lendingRateType, rate, "
                     "comparisonRate, calculationFrequency, applicationFrequency, "
                     "additionalValue, additionalInfo, additionalInfoUri "
                     "FROM BANKINGPRODUCTLENDINGRATE").lower()
        
        masterQuery = ("SELECT ID, LENDINGRATETYPE FROM MASTER_LENDINGRATETYPE").lower()
        
        
        insertQuery = ("INSERT INTO BANKINGPRODUCTLENDINGRATE "
                       "( lendingRateId, Productid, lastUpdated, lendingRateType, rate, "
                       "comparisonRate, calculationFrequency, applicationFrequency, "
                       "additionalValue, additionalInfo, additionalInfoUri, createdOn, "
                       "createdBy, systemCreatedOn, systemCreatedBy ) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").lower()


        
        log.logComment("BankingProductLendingRate (011)   -> Transforming...")
        dataOutput = transform.transformData(masterQuery, tableQuery)
        
        insertData = dataOutput[0]
        insertDataFinal = []
        for d in insertData:
            y = d.split("|")
            if y[4] == '':
                y[4] = '0'
            else:
                y[4] = str("{:.2f}".format(float(d.split("|")[4])))
            #y[4] = str(float(d.split("|")[4]) * 100)
            #y = y + [conn.updatedDate, conn.createdBy, conn.updatedDate, conn.createdBy]
            y = '|'.join(y)
            insertDataFinal.append(y)
        
        seqQuery = "select max(lendingRateId) from BANKINGPRODUCTLENDINGRATE".lower()
        seq = conn.executeQuery(seqQuery)[0][0]
        if seq == None:
            seq = 1
        
        param = {
                'name' : 'bankingProductDeositRate',
                'insertQuery' : insertQuery,
                'insertSelection' : insertDataFinal,
                'size' : 11,
                'seq' : seq
                }
        
        if self.FailInd == 0:
            log.logComment("BankingProductLendingRate (011)   -> Inserting data")
            #transform.insert(**param)
            conn.insertQuery(**param)
            #transform.insert('bankingProductDeositRate', insertQuery, 8, masterQuery, tableQuery, update = False)
            
            conn.closeConnector()
            log.logComment("BankingProductLendingRate (011)   -> Connector Closed")
            
        else:
            log.logComment("BankingProductLendingRate (011)   -> Inserting data -> Failed")
        
        
        
            