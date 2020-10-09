# -*- coding: utf-8 -*-
"""
Created on Wed May 27 10:51:12 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class BankingProductFeature():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("BankingProductFeature (09) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("BankingProductFeature (09) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Feature.csv', 'TeD_Feature.csv', 'TnS_Feature.csv']
        
        transform.df_cols = ['Product ID (Mandatory)', 'CUA Effective From (Mandatory) Date/Time',
                           'Feature Type', 'Value/Details', 'Additional Information', 
                           'Additional Information URL'
                            ]
        
        transform.joinerColumn = 'Feature Type'
        
        tableQuery = ("select Productid, lastUpdated, featureType, additionalValue, "
                      "additionalInfo, additionalInfoUri from BankingProductFeature").lower()
        
        masterQuery = ("SELECT ID, FEATURETYPE FROM MASTER_Features").lower()
        
        
        insertQuery = ("INSERT INTO BankingProductFeature (featureId, Productid, lastUpdated, "
                        "featureType, additionalValue, additionalInfo, additionalInfoUri, "
                        "createdOn, createdBy, systemCreatedOn, systemCreatedBy ) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").lower()

        
        log.logComment("BankingProductFeature (09)   -> Transforming...")
        dataOutput = transform.transformData(masterQuery, tableQuery)
        
        dataOutput = dataOutput[0]
        seqQuery = "select max(cast(featureId as Decimal)) from BankingProductFeature".lower()
        seq = conn.executeQuery(seqQuery)[0][0]
        if seq == None:
            seq = 1
        
        param = {
                'name' : 'bankingProductDeositRate',
                'insertQuery' : insertQuery,
                'insertSelection' : dataOutput,
                'size' : 7,
                'seq' : seq
                }
        
        if self.FailInd == 0:
            log.logComment("BankingProductFeature (09)   -> Inserting data")
            #transform.insert(**param)
            #transform.insert('bankingProductDeositRate', insertQuery, 8, masterQuery, tableQuery, update = False)
            conn.insertQuery(**param)


            conn.closeConnector()
            log.logComment("BankingProductFeature (09)   -> Connector Closed")
            
        else:
            log.logComment("BankingProductFeature (09)   -> Inserting data -> Failed")
        
        
        
            