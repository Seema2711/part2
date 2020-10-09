# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:07:34 2020

@author: Palash
"""

# -*- coding: utf-8 -*-
"""
Created on Wed May 27 10:51:12 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class BankingProductEligibility():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("BankingProductEligibility (08) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("BankingProductEligibility (08) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['TnS_Eligibility.csv', 'TeD_Eligibility.csv', 'CCC_Eligibility.csv']
        
        transform.df_cols = ['Product ID (Mandatory)', 'CUA Effective From (Mandatory) Date/Time',
                             'Eligibility Type', 'Value/Details', 'Additional Information', 'Additional Information URL'
                             ]
        
        transform.joinerColumn = 'Eligibility Type'
        
        tableQuery = ("select ProductID, lastUpdated, eligibilityType, additionalValue, "
                      "additionalInfo, additionalInfoUri from bankingProductEligibility").lower()
        
        masterQuery = ("SELECT ID, ELIGIBILITYTYPE FROM MASTER_ELIGIBILITY").lower()
        
        
        insertQuery = ("INSERT INTO bankingProductEligibility (eligibilityId, ProductID, lastUpdated, "
                       "eligibilityType, additionalValue, additionalInfo, additionalInfoUri, "
                       "createdOn, createdBy, systemCreatedOn, systemCreatedBy ) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").lower()

        insertQuery = ("insert into bankingproducteligibility (eligibilityid, productid, lastupdated, "
                        "eligibilitytype, additionalvalue, additionalinfo, additionalinfouri, createdon, createdby, "
                        "systemcreatedon, systemcreatedby ) "
                        "select * from (select %s as a, %s as product, "
                        "%s as dt, %s as b, %s as c, %s as d, %s as e, "
                        "%s as f, %s as g, %s as h, %s as i from dual) a "
                        "where (a.product, a.dt, a.b, a.c) not in (select productid, lastupdated, eligibilityType, additionalValue  from bankingproducteligibility) ").lower()


        log.logComment("BankingProductEligibility (08)   -> Transforming...")
        dataOutput = transform.transformData(masterQuery, tableQuery)
        dataOutput = dataOutput[0]
        seqQuery = "select max(cast(eligibilityId as decimal)) from bankingProductEligibility".lower()
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
            log.logComment("BankingProductEligibility (08)   -> Inserting data")
            #transform.insert(**param)
            #transform.insert('bankingProductDeositRate', insertQuery, 8, masterQuery, tableQuery, update = False)
            conn.insertQuery(**param)

            conn.closeConnector()
            log.logComment("BankingProductEligibility (08)   -> Connector Closed")
            
        else:
            log.logComment("BankingProductEligibility (08)   -> Inserting data -> Failed")
        
        
        
            