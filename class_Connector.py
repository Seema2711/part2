# -*- coding: utf-8 -*-
"""
Created on Tue May 26 22:19:15 2020

@author: Palash

Import Command: from class_Connector import Connector
Initialize as : x = Connector(root, 'palash')
"""

import mysql.connector
import configparser
import datetime
from time import sleep

class Connector():
    
    def __init__(self, rootFolder, CreatedBy, log, UpdatedBy = None):
        self.log = log
        self.log.logComment("Connector Class -> Initialized")
        connectionDetails = rootFolder + '/' + 'DBCONFIG.INI'
        config = configparser.ConfigParser()
        config.read(connectionDetails)
        
        
        # Audit parameters
        self.createdBy = CreatedBy
        self.updatedBy = UpdatedBy
        d = datetime.datetime.now()
        self.updatedDate =  '-'.join([str(d.year), str(d.month), str(d.day)])

        # Connection config
        self.ConnConfig = {'user': config["DEFAULT"]["Username"],
                      'password': config["DEFAULT"]["Password"],
                      'host': config["DEFAULT"]["Host"],
                      'database': config["DEFAULT"]["Schema"],
                      'raise_on_warnings': bool(config["DEFAULT"]["raise_on_warnings"])
                      }
        
        
    def openConnector(self):
        self.log.logComment("Connector Class -> openConnector Called")
        # Setup a connection
        try:
            self.cnx = mysql.connector.connect(**self.ConnConfig)
            # Create MySQL query Cursor
            self.cursor = self.cnx.cursor()
            self.log.logComment("Connector Class -> openConnector Successful")
            return 0
            
        except:
            self.log.logComment("Connector Class -> Could not connect")
            return 1
        
    def insertQuery(self, name, insertQuery, insertSelection, size, audit = True, seq = 0):        
        self.log.logComment("Connector Class -> insertQuery Called")
        # Insert Sequence
        reject = []
        
        if len(insertSelection) > 0:
            for val in insertSelection:
                val = val.replace("|False|","|0|")
                val = val.replace("|nan","|")
                val = val.replace("|None","|")
                d = val.split('|')
                
                if(audit):
                    d = d[:size]
                    d = d + [self.updatedDate, self.createdBy, self.updatedDate, self.createdBy]
                else:
                    d1 = []
                    for i in d:
                        if(i in ['', 'None', 'nan']):
                            d1.append(None)
                        else:
                            d1.append(i)
                    d = d1
                
                if(seq != 0):
                    d = [str(seq)] + d
                    seq = int(seq) + 1
                
                
                try:
                    print(d)
                    self.cursor.execute(insertQuery, d)
                
                except:
                    reject.append(d)
                 
            #log value
            log = {'status' : 'Insert Success',
                   'Rows inserted' : len(insertSelection)
                   }
        else:
            log = {'status' : 'Insert Success',
                   'Rows inserted' : len(insertSelection)
                   }
        
        # committed to the database
        self.cnx.commit()
        self.log.logComment("Connector Class -> insertQuery Success and Commited")
        self.log.logComment("Insert reject data: ")
        self.log.logComment(reject)
        
    def updateQuery(self, name, updateQuery, updateSelection, size):
        self.log.logComment("Connector Class -> updateQuery Called")
        # Update Sequence
        reject_update = []
        
        
        if len(updateSelection) > 0:
            for val in updateSelection:
                val = val.replace("|False|","|0|")
                val = val.replace("|nan","|")
                val = val.replace("|None","|")
                d = val.split('|')
                d1 = d[3 : size-2] + [self.updatedDate, self.updatedBy] + d[:3]
                
                try:
                    self.cursor.execute(updateQuery, d1)
                except:
                    reject_update.append(val)
                    
            #log value
            logU = {'status' : 'Update Success',
                   'Rows inserted' : len(insertSelection)
                   }
        else:
            logU = {'status' : 'Update Success',
                   'Rows inserted' : len(insertSelection)
                   }
        
        # committed to the database
        self.cnx.commit()
        self.log.logComment("Connector Class -> updateQuery Success and Commited")
        self.log.logComment("Update reject data: ")
        self.log.logComment(reject_update)
        
    def masterInsert(self,insert_productCategory, insertStatement, seq_id):
        self.log.logComment("Connector Class -> masterInsert Called")
        if len(insert_productCategory) > 0:
            for val in insert_productCategory:
                if (val not in ['None', 'nan']):
                    self.cursor.execute(insertStatement, (seq_id,val))
                    seq_id += 1
            
                #log value
                log = {'status' : 'Insert Success',
                       'Rows inserted' : len(insert_productCategory)
                       }
        else:
            log = {'status' : 'Insert Success',
                   'Rows inserted' : len(insert_productCategory)
                   }
        self.cnx.commit()
        self.log.logComment("Connector Class -> masterInsert Success and Commited")
        
    def executeQuery(self, query, returnVal = True):
        self.log.logComment("Connector Class -> executeQuery Called")
        self.cursor.execute(query)
        self.log.logComment("Connector Class -> executeQuery Success ")
        if returnVal:
            return self.cursor.fetchall()
        else:
            return 0
    
    def insertSingleRow(self, query, dataList):
        
        try:
            self.cursor.execute(query, dataList)
            return 0
        except mysql.connector.IntegrityError:
            return 0
        except:
            return 1
    
    def closeConnector(self):
        self.log.logComment("Connector Class -> closeConnector Called")
        self.cnx.commit()
        self.cnx.close()
        self.log.logComment("Connector Class -> closeConnector Success ")
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        