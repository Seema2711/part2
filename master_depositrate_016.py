# -*- coding: utf-8 -*-
"""
Created on Wed May 27 00:53:22 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class MasterdepositRate():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized MasterdepositRate (016)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("MasterdepositRate (016) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['TnS_Rate.csv','RatesFile.csv']
        
        masterQuery = "SELECT ID, DEPOSITRATETYPE FROM MASTER_DEPOSITRATETYPE".lower()
        
        insertStatement = ("INSERT INTO MASTER_DEPOSITRATETYPE "
                           "(ID, DEPOSITRATETYPE) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'Rate Type (Mandatory)'
        
        if self.FailInd == 0:
            log.logComment("MasterdepositRate (016) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment("MasterdepositRate (016) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment("MasterdepositRate (016) -> Inserting data -> Failed")