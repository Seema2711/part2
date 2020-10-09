# -*- coding: utf-8 -*-
"""
Created on Wed May 27 10:23:55 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class BankingAdditionalInfo():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("BankingAdditionalInfo (01) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("BankingAdditionalInfo (01) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        
        query = ("SELECT PRODUCTID, OVERVIEWURI, TERMSURI, ELIGIBILITYURI, FEESANDPRICINGURI,"
         "BUNDLEURI, lastupdated from BANKINGADDITIONALINFORMATION").lower()

        # Rearrange DF columns
        
        df_cols = ['Product ID (Mandatory)', 'Overview URL', 'Terms URL',
                   'Eligibility URL', 'Fees And PricingURL', 'Bundle URL', 'CUA Effective From (Mandatory) Date/Time'
                   ]
        
        productFiles = ['TeD_Products.csv', 'TnS_Products.csv', 'CCC_Products.csv']
        
        conn.openConnector()
        conn.cursor.execute(query)
        
        product_table  = conn.cursor.fetchall()
        
        import pandas as pd
        product_table = pd.DataFrame(product_table)
        
        #####
        firstRun = 1
        
        for file in productFiles:
            if firstRun == 1:
                product = transform.getDF(file)    
                product = product[df_cols]
                firstRun = 0
            else:
                newFile = transform.getDF(file)
                newFileCols = newFile.columns
                productCols = product.columns
                sizeofNewFile = len(newFile)
                sizeofProduct = len(product)
                
                
                # add any column which new file do not have
                for column in df_cols:
                    if column not in newFileCols:
                        addColumn = {column : [None] * sizeofNewFile}
                        newFile[column] = pd.Series(addColumn)
        
                # add any column which product file do not have
                for column in newFileCols:
                    if column not in productCols:
                        addColumn = {column : [None] * sizeofProduct}
                        product[column] = pd.Series(addColumn)
                
                # arrange columns in new DF
                newFile = newFile[df_cols]
                product = product[df_cols]
                
                # merge into product df
                product = pd.concat([product, newFile])
        #####
        
        # Accumulate all rows into one ordered string
        
        insertData = []
        
        for i in range(0, len(product)):
            insertData.append('|'.join([str(x) for x in product.iloc[[i],:].values.tolist()[0]]))
            
            
        # Gather data already present in table in var(insertData) format
            
        tableData = []
        
        for i in range(0, len(product_table)):
            # tableData.append('|'.join(product_table.iloc[[i],:].values.tolist()[0]))
            # Convert all elements to STR as join does not work on timestamp
            d = [ str(x) for x in product_table.iloc[[i],:].values.tolist()[0] ]
            tableData.append('|'.join(d))
            
            
        # Identify data needs to be inserted
        insertSelection = []
        
        for insertDat in insertData:
            if insertDat not in tableData:
                insertSelection.append(insertDat)
                
        insertQuery = ("INSERT INTO BANKINGADDITIONALINFORMATION "
                       "(PRODUCTID, OVERVIEWURI, TERMSURI, ELIGIBILITYURI, FEESANDPRICINGURI,"
                       "BUNDLEURI, lastupdated) VALUES (%s, %s, %s, %s, %s, %s, %s)").lower()
        
        
        reject = []
        
        if len(insertSelection) > 0:
            for val in insertSelection:
                val = val.replace("|False|","|0|")
                val = val.replace("|nan","|")
                val = val.replace("|None","|")
                d = val.split('|')
                if(d[1] in ['NaT', 'nan']):
                    d[1] = None
                if(d[2] in ['NaT', 'nan']):
                    d[2] = None
                    
                #cursor.execute(insertQuery, (d[0], d[1], d[2], d[3], d[4], d[5]))
                
                try:
                    conn.cursor.execute(insertQuery, (d[0], d[1], d[2], d[3], d[4], d[5], d[6]))
                except:
                    reject.append(val)
                
                
            #log value
            log = {'status' : 'Insert Success',
                   'Rows inserted' : len(insertSelection)
                   }
        else:
            log = {'status' : 'Insert Success',
                   'Rows inserted' : len(insertSelection)
                   }
        
        # committed to the database
        conn.closeConnector()
