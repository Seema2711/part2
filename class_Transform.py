# -*- coding: utf-8 -*-
"""
Created on Tue May 26 23:35:44 2020

@author: Palash
"""

import pandas as pd
import numpy as np

class Transform():
    
    def __init__(self, ConnectorClassObj, root, log):
        
        log.logComment("Transform Class -> Initialized")
        self.cnx = ConnectorClassObj
        self.root = root
        self.fileList = []
        self.df_cols = []
        self.joinerColumn = ''
        self.cnx.openConnector()
        self.log = log
        
    def getDF(self, filePath):
        dict = {}
        
        setupDict = 1
        
        filePath = self.root + '/ExportDF/' + filePath
        
        try:
            with open(filePath,'r') as f:
                for l in f:
                    if setupDict == 1:
                        for key in l.split("|"):
                            dict.update({key.strip()[1 : -1] : []})
                        setupDict = 0
                    else:
                        i = 0
                        
                        keys = list(dict.keys())
                        
                        for val in l.split("|"):
                            if val in ['None', 'nan']:
                                dict[keys[i]].append('')
                                
                            else:
                                try:
                                    dict[keys[i]].append(val.strip()[1:-1])
                                except:
                                    dict[keys[i]].append('')
                            i += 1
        
        except:
            #return []
            return pd.DataFrame({})
    
        df = pd.DataFrame(dict)
        
        return df


    def joinFiles(self, masterFeeDF):
        self.log.logComment("Transform Class -> joinFiles function called")
        
        if masterFeeDF.empty:
            masterFeeDF = pd.DataFrame({0 : ['0'], 1 : ['0']})
            
        #####
        firstRun = 1
        
        for file in self.fileList:
            if firstRun == 1:
                product = self.getDF(file)
                product = pd.merge(product, masterFeeDF, how = 'left', left_on = self.joinerColumn, right_on =  1)
                product = product[self.df_cols]
                firstRun = 0
            else:
                newFile = self.getDF(file)
                
                if len(newFile) > 0:
                
                    newFile = pd.merge(newFile, masterFeeDF, how = 'left', left_on = self.joinerColumn, right_on =  1)
                    newFileCols = newFile.columns
                    productCols = product.columns
                    sizeofNewFile = len(newFile)
                    sizeofProduct = len(product)
                    
                    
                    # add any column which new file do not have
                    for column in self.df_cols:
                        if column not in newFileCols:
                            addColumn = {column : [None] * sizeofNewFile}
                            newFile[column] = pd.Series(addColumn)
            
                    # add any column which product file do not have
                    for column in newFileCols:
                        if column not in productCols:
                            addColumn = {column : [None] * sizeofProduct}
                            product[column] = pd.Series(addColumn)
                    
                    # arrange columns in new DF
                    newFile = newFile[self.df_cols]
                    product = product[self.df_cols]
                    
                    # merge into product df
                    product = pd.concat([product, newFile])
        
        if(0 in product.columns):
            cols = product.columns.tolist()
            setCols = set(cols)
            if(len(cols) == len(setCols)):
                product[0] = product[0].fillna('-1')
                product.drop(product.loc[product[0] == '-1'].index, inplace = True)
                product[0] = product[0].apply(int)
                product[0] = product[0].apply(str)
            else:
                product[0] = product[0].fillna('-1')
                cols = product.columns.tolist()
                cols[3] = '0_x'
                product.columns = cols
                product.drop(product.loc[product[0] == '-1'].index, inplace = True)
                product[0] = product[0].apply(int)
                product[0] = product[0].apply(str)
                product['0_x'] = product['0_x'].apply(int)
                product['0_x'] = product['0_x'].apply(str)
        
        self.log.logComment("Transform Class -> joinFiles function Success")
        
        return product
        

    # This function will return insertSelection and updateSelection
    def transformData(self, masterQuery, tableQuery):
        self.log.logComment("Transform Class -> transformData function called")
        masterFeeDF = pd.DataFrame(self.cnx.executeQuery(masterQuery))
        product_table = pd.DataFrame(self.cnx.executeQuery(tableQuery))
        product = self.joinFiles(masterFeeDF)
        
        # For debugging uncomment below to see data read from file
        #self.log.logComment(product)
        
        # Accumulate all rows into one ordered string
        
        insertData = []
        
        for i in range(0, len(product)):
            insertData.append('|'.join([str(x) for x in product.iloc[[i],:].values.tolist()[0]]))
            
        
        insertData = set(insertData)
        
          
        # Gather data already present in table in var(insertData) format
            
        tableData = []
        
        
        for i in range(0, len(product_table)):
            # tableData.append('|'.join(product_table.iloc[[i],:].values.tolist()[0]))
            # Convert all elements to STR as join does not work on timestamp
            d = [ str(x) for x in product_table.iloc[[i],:].values.tolist()[0] ]
            dt = d[1]
            d[1] = "/".join([dt[8:10], dt[5:7], dt[0:2]])
            tableData.append('|'.join(d))
            
            
        # Identify data needs to be inserted
            
        insertSelection = []
        updateSelection = []
        
        for insertDat in insertData:
            insertDat = insertDat.replace("|False|","|0|")
            insertDat = insertDat.replace("|nan","|")
            insertDat = insertDat.replace("|None","|")
            
            inserted = 1
            
            for tabData in tableData:
                inserted = 1
                if set(tabData.split("|")[0:3]) == set(insertDat.split("|")[0:3]):
                    if set(tabData.split("|")[3:]) != set(insertDat.split("|")[3:]):
                        updateSelection.append(insertDat)
                    inserted = 0
                    break
            
            if inserted == 1:
                insertSelection.append(insertDat)
        
        self.log.logComment("Transform Class -> transformData function Success - data Insert-update")
        # self.log.logComment(insertSelection)
        # self.log.logComment(updateSelection)
        
        return [insertSelection, updateSelection]
        
    def insert(self, name, insertQuery, size, masterQuery, tableQuery, update = False, audit = True):
        
        # reducing 4 from size to compensate audit columns
        if(audit):
            size = size - 4
        
        selection = self.transformData(masterQuery, tableQuery)
        insertSelection = set(selection[0])
        updateSelection = set(selection[1])
        self.cnx.insertQuery(name, insertQuery, insertSelection, size, audit)
        
        if(update):
            self.cnx.insertQuery(name, insertQuery, updateSelection, size)
    
    def masterInsert(self, masterQuery, insertStatement):
        self.log.logComment("Transform Class -> masterInsert function called")
        
        product_category_tab = pd.DataFrame(self.cnx.executeQuery(masterQuery))
        
        if product_category_tab.empty:
            seq_id = 1
            productCategoryList = []
            
        else:
            seq_id = int(product_category_tab.iloc[:,[0]].max().values[0] + 1)
        
        
            productCategoryList = []
        
            for productCat in product_category_tab.iloc[:,[1]].values.tolist():
                productCategoryList.append(productCat[0])
        
        insert_productCategory = []
        
        for productFile in self.fileList:
            product = self.getDF(productFile)
            
            if len(product) == 0:
                continue
            
            insert_productCategory += product[self.joinerColumn].unique().tolist()
        
        #seq_array = range(seq_id, seq_id + len(insert_productCategory))
        
        # remove existing values
        vals = []
        for val in insert_productCategory:
            if(val not in productCategoryList):
                vals.append(val)
        
        insert_productCategory = set(vals)
        
        self.cnx.masterInsert(insert_productCategory, insertStatement, seq_id)
        self.log.logComment("Transform Class -> masterInsert function Success - data Inserted")
        
        
        
        