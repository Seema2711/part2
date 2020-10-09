# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:17:05 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class MasterFee():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized MasterFee (021)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("MasterFee (021) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Fees.csv', 'TeD_Fees.csv', 'TnS_Fees.csv']
        
        masterQuery = ("SELECT ID, FEETYPE FROM MASTER_FEES").lower()
        
        insertStatement = ("INSERT INTO MASTER_FEES "
                   "(ID, FEETYPE) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'Fee Name'
        
        if self.FailInd == 0:
            log.logComment("MasterFee (021) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment("MasterFee (021) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment("MasterFee (021) -> Inserting data -> Failed")