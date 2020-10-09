# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:17:05 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class MasterConstraint():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized MasterConstraint (015)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("MasterConstraint (015) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Constraints.csv', 'TeD_Constraints.csv', 'TnS_Constraints.csv']
        
        masterQuery = ("SELECT ID, CONSTRAINTTYPE FROM MASTER_CONSTRAINTS").lower()
        
        insertStatement = ("INSERT INTO MASTER_CONSTRAINTS "
                           "(ID, CONSTRAINTTYPE) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'Constraint Type'
        
        if self.FailInd == 0:
            log.logComment("MasterConstraint (015) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment("MasterConstraint (015) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment("MasterConstraint (015) -> Inserting data -> Failed")