# -*- coding: utf-8 -*-
"""
Created on Fri May  1 18:00:45 2020

@author: palas
"""

import pandas as pd
from DataOperations import DOP

class splitProduct:
    
    def __init__(self, dataFile, sheet):
        try:
            self.df = pd.read_excel(dataFile, sheet_name = sheet)
        except:
            self.df = pd.DataFrame({})
        #self.indexes = df.loc[df["Standard Term Deposits"] == "Product ID (Mandatory)"].index
        self.dict = {}
        self.path = ""
        self.log = ""
        
    def createDict(self):
        
        indexes = self.df.loc[self.df["Standard Term Deposits"] == "Product ID (Mandatory)"].index
        
        i = 0
        for val in range(0, len(indexes)):
            begin = indexes[val] + 2
            productID = self.df.iloc[[begin - 2], [2]].values[0][0]
            productName = self.df.iloc[[begin - 1], [2]].values[0][0]
            
            productDF = pd.DataFrame({'Standard Term Deposits':['Product ID', 'Product Name'], 'ProductCol2':[productID, productName]})
            
            try:
                end = indexes[val + 1]
            except IndexError:
                end = len(self.df)
            i += 1
            subDF = self.df.iloc[begin:end, :]
            
            #subDF = subDF.dropna(subset = ["Standard Term Deposits"])
            #subDF = pd.concat([subDF, productDF])
            
            j = 2
            
            while j < len(subDF.columns):
                insertDF = subDF.iloc[:, [1, j]]
                insertDF = pd.concat([insertDF, productDF.rename(columns = {"ProductCol2":insertDF.columns.values.tolist()[1]})])
                #insertDF = pd.concat([insertDF, productDF])
                self.insertDict(insertDF)
                j += 1
        retDF = pd.DataFrame(self.dict)
        self.exportCSV(retDF, "RatesFile")
                
    def insertDict(self, insertDF):
        insertDF = insertDF.dropna(subset = ["Standard Term Deposits"])

        keys = insertDF[insertDF.columns.values[0]].values.tolist()
        vals = insertDF[insertDF.columns.values[1]].values.tolist()
        
        import numpy as np
        
        # Key Existance check
        
        for key in keys:
            if key not in self.dict.keys():
                if len(self.dict.keys()) > 0:
                    firstKey = next(iter(self.dict.keys()))
                    #print(key)
                    nullArray = [np.nan] * len(self.dict[firstKey])
                    self.dict.update({key : nullArray})
                else:
                    self.dict.update({key : []})
  
        # Add data
        i = 0
        for key in keys:
            try:
                self.dict[keys[i]].append(vals[i])
            except AttributeError:
                self.dict[keys[i]].append("")
            i += 1
            
        # not updated keys
        leftKeys = [item for item in self.dict.keys() if item not in keys]
        for key in leftKeys:
            self.dict[key].append('')
    
    def exportCSV(self, df, name):
        self.log.logComment("fn: exportCSV  - start")  
        path = self.path + "/" + name + ".csv"
        #df.to_csv(path)
        self.createCSV(df, path)
        self.log.logComment("fn: exportCSV  - finish")
    
    def stringConverter(self, column):
        columnStr = ""
        for key in column:
            columnStr = columnStr + '"'+ str(key) + '"|'
        return columnStr[:-1]
    
    def createCSV(self, df, path):
        column = df.to_dict().keys()
        rows = df.values.tolist()
        fileCSV = []
        fileCSV.append(self.stringConverter(column))
        for line in rows:
            fileCSV.append(self.stringConverter(line))
        
        with open(path, "w") as f:
            for line in fileCSV:
                f.write(line + '\n')
        