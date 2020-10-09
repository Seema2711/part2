# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:17:05 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class MasterFeature():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized MasterFeature (020)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("MasterFeature (020) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Feature.csv', 'TeD_Feature.csv', 'TnS_Feature.csv']
        
        masterQuery = ("SELECT ID, FEATURETYPE FROM MASTER_FEATURES").lower()
        
        insertStatement = ("INSERT INTO MASTER_FEATURES "
                           "(ID, FEATURETYPE) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'Feature Type'
        
        if self.FailInd == 0:
            log.logComment("MasterFeature (020) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment("MasterFeature (020) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment("MasterFeature (020) -> Inserting data -> Failed")