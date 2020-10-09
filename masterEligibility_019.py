# -*- coding: utf-8 -*-
"""
Created on Wed May 27 11:57:06 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class MasterEligibility():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized  MasterEligibility (019)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment(" MasterEligibility (019) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['TnS_Eligibility.csv', 'TeD_Eligibility.csv', 'CCC_Eligibility.csv']
        
        masterQuery = "select id, eligibilityType from master_eligibility".lower()
        
        insertStatement = ("INSERT INTO master_eligibility "
                           "(ID, eligibilityType) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'Eligibility Type'
        
        if self.FailInd == 0:
            log.logComment(" MasterEligibility (019) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment(" MasterEligibility (019) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment(" MasterEligibility (019) -> Inserting data -> Failed")