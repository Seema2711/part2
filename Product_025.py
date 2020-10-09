# -*- coding: utf-8 -*-
"""
Created on Wed May 27 03:01:50 2020

@author: Palash
"""


from class_Connector import Connector
from class_Transform import Transform

class Product():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("Product (025) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("Product (025) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['TeD_Products.csv', 'TnS_Products.csv', 'CCC_Products.csv']
        
        transform.df_cols = ['Product ID (Mandatory)', 'productCategory (Mandatory)', 'CUA Effective From (Mandatory) Date/Time',
                             'Effective From', 'Effective To', 'Product Name (Mandatory)', 'Description', 'Apply Here URL (Mandatory)',
                             'isTailored (Mandatory)', 'Overview URL', 'Terms URL', 'Eligibility URL', 'Fees And PricingURL',
                             'Bundle URL'
                             ]


        
        transform.joinerColumn = 'productCategory (Mandatory)'
        
        tableQuery = ("select productId, productCategory, lastUpdated, "
                      "effectiveFrom, effectiveTo, name, description, "
                      "applicationUri, isTailored, "
                      "overviewUri, termsUri, eligibilityUri, feesAndPricingUri, "
                      "bundleUri from product").lower()
        
        masterQuery = ("SELECT ID, PRODUCTCATEGORY FROM PRODUCT_CATEGORY").lower()
        
        
        insertQuery = ("INSERT INTO product (productId, productCategory, lastUpdated, "
                        "effectiveFrom, effectiveTo, name, description, applicationUri, isTailored,"
                        "overviewUri, termsUri, eligibilityUri, feesAndPricingUri, bundleUri) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").lower()
        
        
        log.logComment("Product (025) -> Transforming...")
        transform.transformData(masterQuery, tableQuery)
        
        param = {
                'name' : 'bankingProductDeositRate',
                'insertQuery' : insertQuery,
                'size' : 14,
                'masterQuery' : masterQuery,
                'tableQuery' : tableQuery,
                'update' : False,
                'audit' : False
                }
        
        if self.FailInd == 0:
            log.logComment("Product (025) -> Inserting data")
            transform.insert(**param)
            #transform.insert('bankingProductDeositRate', insertQuery, 8, masterQuery, tableQuery, update = False)
            
            conn.executeQuery("update product set brand = 'CUA', brandname = 'CUA' where brand is null", returnVal = False)
            
            conn.closeConnector()
            log.logComment("Product (025) -> Connector Closed")
            
        else:
            log.logComment("Product (025) -> Inserting data -> Failed")
        
        
        
            