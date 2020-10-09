# -*- coding: utf-8 -*-
"""
Created on Wed May 27 00:17:59 2020

@author: Palash
"""

from class_Connector import Connector
from class_Transform import Transform

class BankingProductdepositRateTwo():
    
    def __init__(self, root, RunByUsername, log):
        #root = "E:/CUA OpenBank API/OpenBanking/ETL"
        
        log.logComment("BankingProductdepositRate (04-02) -> Initialized")
        conn = Connector(root, RunByUsername, log)
        
        self.FailInd = conn.openConnector()
        
        log.logComment("BankingProductdepositRate (04-02) -> Connection Status: " + str(self.FailInd))
        
        transform = Transform(conn, root, log)
        
        transform.fileList = ['RatesFile.csv']
        
        transform.df_cols = ["Product ID", "Product Name",'Rate Type (Mandatory)', 
                             'Rate (Mandatory)', 'calculationFrequency (Auto Generated)', 
                             "Additional Value (Auto Generated)", 'Additional Details'
                             ]
        
        transform.joinerColumn = 'Rate Type (Mandatory)'
        
        tableQuery = ("select Productid, lastUpdated, depositRateType, "
                 "rate, calculationFrequency,applicationFrequency, "
                 "additionalValue from bankingproductdepositRate").lower()
        
        masterQuery = ("SELECT ID, DEPOSITRATETYPE FROM MASTER_DEPOSITRATETYPE").lower()
        
        
        insertQuery = ("INSERT INTO bankingproductdepositRate (depositRateId, Productid, lastUpdated, "
                        "depositRateType, rate, calculationFrequency,applicationFrequency, additionalValue, "
                        " createdOn, createdBy, systemCreatedOn, systemCreatedBy ) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").lower()
        
        
        insertQuery = ("insert into bankingproductdepositrate (depositrateid, productid, lastupdated, "
                        "depositratetype, rate, calculationfrequency,applicationfrequency, additionalvalue, "
                        "createdon, createdby, systemcreatedon, systemcreatedby ) "
                        "select * from (select %s as a, %s as productid, %s lastupdated, %s as b, %s as c, %s as d, %s as e, "
                        "%s as f, %s as g, %s as h, %s as i, %s as j from dual) a "
                        "where (a.productid, a.lastupdated, a.b) not in (select productid, lastUpdated, depositratetype from bankingproductdepositrate)").lower()

        
        updateQuery = ( "update bankingproductdepositRate set depositRateType = %s, "
                        "rate = %s, calculationFrequency = %s, applicationFrequency = %s, "
                        "additionalValue = %s, systemCreatedOn = %s, systemCreatedBy = %s "
                        "where Productid = %s and depositRateId = %s and lastupdated = %s"
                        )
        
        log.logComment("BankingProductdepositRate (04-02) -> Transforming...")
        try:
            dataOutput = transform.transformData(masterQuery, tableQuery)
        except:
            dataOutput = [[], []]
        
        insertData = dataOutput[0]
        insertDataFinal = []
        for d in insertData:
            y = d.split("|")
            try:
                y[3] = str("{:.2f}".format(float(d.split("|")[3]) * 100))
            except:
                y[3] = '0'
            #y[4] = str(float(d.split("|")[4]) * 100)
            #y = y + [conn.updatedDate, conn.createdBy, conn.updatedDate, conn.createdBy]
            y = '|'.join(y)
            insertDataFinal.append(y)
        
        seqQuery = "select max(cast(depositRateId as decimal)) + 1 from bankingproductdepositrate;".lower()
        seq = conn.executeQuery(seqQuery)[0][0]
        if seq == None:
            seq = 1
        
        param = {
                'name' : 'bankingProductDeositRate',
                'insertQuery' : insertQuery,
                'insertSelection' : insertDataFinal,
                'size' : 8,
                'seq' : seq
                }
        from time import sleep
        sleep(3)
        
        if self.FailInd == 0:
            log.logComment("BankingProductdepositRate (04-02) -> Inserting data")
            #transform.insert(**param)
            #transform.insert('bankingProductDeositRate', insertQuery, 8, masterQuery, tableQuery, update = False)
            conn.insertQuery(**param)
            conn.closeConnector()
            log.logComment("BankingProductdepositRate (04-02) -> Connector Closed")
            
        else:
            log.logComment("BankingProductdepositRate (04-02) -> Inserting data -> Failed")
        
        
        
            