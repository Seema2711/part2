# -*- coding: utf-8 -*-
"""
Created on Wed May 27 00:53:22 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform
import pandas as pd

class CardArt():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        log.logComment("Initialized Card Art")
        
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("Initialized Card Art -> Connection Status: " + str(self.FailInd))
        
        
        transform = Transform(conn, root, log)
        
        transform.fileList = files = ['TnS_Card.csv', 'CCC_Card.csv']
        
        masterQuery = "select cardArtId, productId, title, imageUri from bankingproductcardart".lower()
        
        insertStatement = ("INSERT INTO bankingproductcardart "
                           "(cardArtId, productId, lastUpdated, title, imageUri ) VALUES (%s, %s, %s, %s, %s)").lower()
        
        re_df_cols = ['Card Type', 'image URL (Mandatory if Card Art available)',
                      'Product ID (Mandatory)', 'CUA Effective From (Mandatory) Date/Time']
        df_col = ['Product ID (Mandatory)', 'CUA Effective From (Mandatory) Date/Time','Card Type', 'image URL (Mandatory if Card Art available)']
        
        column_dict = {
        "Card Type" : "Card Type",
        "Product ID (Mandatory)" : "Product ID (Mandatory)",
        "CUA Effective From (Mandatory) Date/Time" : "CUA Effective From (Mandatory) Date/Time",
        "image URL (Mandatory if Card Type selected )" : "image URL (Mandatory if Card Art available)",
        "image URL (Mandatory if Card Art available)" : "image URL (Mandatory if Card Art available)"
        }
        
        
        cardDFi = ''
        first = 0
        for file in files:
            cardDF = transform.getDF(file)
            #cardDF.columns = re_df_cols
            df_cols = []
        
            for col in cardDF.columns:
                if(col in column_dict.keys()):
                    df_cols.append(column_dict[col])
                else:
                    df_cols.append(col)
            cardDF.columns = df_cols
            cardDF = cardDF[df_cols]
            if first == 0:
                cardDFi = cardDF
                first = 1
            else:
                cardDFi = pd.concat([cardDFi, cardDF])
        
        cardDFi = cardDFi[df_col]

        
        masterOutput = pd.DataFrame(conn.executeQuery(masterQuery))
        
        seqQuery = " select max(cast(cardartID as signed)) + 1 from bankingproductcardart".lower()
        seq_id = conn.executeQuery(seqQuery)[0][0]
        if seq_id == None:
            seq_id = 1
        
        
        reject = []
        for DataLines in cardDFi.values.tolist():
            data = [seq_id] + DataLines
            if str(DataLines[2]) not in ['nan']:
                d = []
                for dat in data:
                    if str(dat) in ['nan','None','none','NaN',None]:
                        d.append('')
                    else:
                        d.append(dat)
                
                data = d
                res = conn.insertSingleRow(insertStatement, data)
                seq_id += 1
                if res == 1:
                    reject.append(data)
            
        conn.closeConnector()
        
        log.logComment("Reject: ")
        log.logComment(reject)
        