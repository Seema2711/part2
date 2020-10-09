# -*- coding: utf-8 -*-
"""
Created on Wed May 27 02:54:54 2020

@author: Palash
"""
# -*- coding: utf-8 -*-
"""
Created on Wed May 27 00:53:22 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class ProductCategory():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized ProductCategory (026)")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("ProductCategory (026) -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['CCC_Products.csv', 'TeD_Products.csv', 'TnS_Products.csv']
        
        masterQuery = "SELECT ID, PRODUCTCATEGORY FROM PRODUCT_CATEGORY".lower()
        
        insertStatement = ("INSERT INTO PRODUCT_CATEGORY "
                           "(ID, PRODUCTCATEGORY) VALUES (%s, %s)").lower()
        
        transform.joinerColumn = 'productCategory (Mandatory)'
        
        if self.FailInd == 0:
            log.logComment("ProductCategory (026) -> Inserting data")
            transform.masterInsert(masterQuery, insertStatement)
            
            log.logComment("ProductCategory (026) -> Connection Closed")
            conn.closeConnector()
        else:
            log.logComment("ProductCategory (026) -> Inserting data -> Failed")
