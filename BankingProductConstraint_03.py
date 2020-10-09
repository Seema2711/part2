# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:16:59 2020

@author: Palash
"""

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

class BankingProductConstraint():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("BankingProductConstraint (03) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("BankingProductConstraint (03) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['TeD_Constraints.csv', 'CCC_Constraints.csv', 'TnS_Constraints.csv']
        
        transform.df_cols = ['Product ID (Mandatory)', 'CUA Effective From (Mandatory) Date/Time',
                             'Constraint Type', 'Value/Details', 'Additional Information', 'Additional Information URL'
                             ]
        
        transform.joinerColumn = 'Constraint Type'
        
        tableQuery = ("select Productid, lastUpdated, constraintType, additionalValue, "
                      "additionalInfo, additionalInfoUri from bankingProductConstraint").lower()
        
        masterQuery = ("SELECT ID, CONSTRAINTTYPE FROM MASTER_CONSTRAINTS").lower()
        
        
        insertQuery = ("INSERT INTO bankingProductConstraint (constraintId, Productid, lastUpdated, "
                       "constraintType, additionalValue, additionalInfo, additionalInfoUri, "
                       "createdOn, createdBy, systemCreatedOn, systemCreatedBy ) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").lower()
        
        insertQuery = ("insert into bankingproductconstraint (constraintid, productid, lastupdated, constrainttype, "
                        "additionalvalue, additionalinfo, additionalinfouri, createdon, createdby, systemcreatedon, "
                        "systemcreatedby ) "
                        "select * from (select %s as a, %s as productid, %s lastupdated, %s as b, %s as c, %s as d, %s as e, "
                        "%s as f, %s as g, %s as h, %s as i from dual) a "
                        "where (a.productid, a.lastupdated, a.b, a.c) not in (select productid, lastUpdated, constrainttype, additionalvalue from bankingproductconstraint)").lower()


        
        log.logComment("BankingProductConstraint (03)   -> Transforming...")
        dataOutput = transform.transformData(masterQuery, tableQuery)
        dataOutput = dataOutput[0]
        #dataOutput = list(set(dataOutput))
        seqQuery = "select max(constraintId) from bankingProductConstraint"
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
            log.logComment("BankingProductConstraint (03)   -> Inserting data")
            #transform.insert(**param)
            #transform.insert('bankingProductDeositRate', insertQuery, 8, masterQuery, tableQuery, update = False)
            
            conn.insertQuery(**param)
            
            conn.closeConnector()
            log.logComment("BankingProductConstraint (03)   -> Connector Closed")
            
        else:
            log.logComment("BankingProductConstraint (03)   -> Inserting data -> Failed")
        
        
        
            