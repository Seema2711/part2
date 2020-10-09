# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:17:05 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class MasterLendingRate():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized MasterLendingRate (022)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("MasterLendingRate (022) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Rate.csv']
        
        masterQuery = ("SELECT ID, LENDINGRATETYPE FROM MASTER_LENDINGRATETYPE").lower()
        
        insertStatement = ("INSERT INTO MASTER_LENDINGRATETYPE "
                   "(ID, LENDINGRATETYPE) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'Rate Type (Mandatory)'
        
        if self.FailInd == 0:
            log.logComment("MasterLendingRate (022) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment("MasterLendingRate (022) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment("MasterLendingRate (022) -> Inserting data -> Failed")