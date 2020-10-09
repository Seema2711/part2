# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 12:20:31 2020

@author: palas
"""

import pandas as pd
import datetime
import numpy as np
from class_Connector import Connector

class ValidationCheck():
    
    def __init__(self, sourceFile, validationChecklist, exportpath,root, log = None):
        #sourceFile = 'F:/CUA OpenBank API/OpenBanking/Integrated-v-1-4/Data/PRODUCTS_V2 - Ver 0.4 for July 2020.xlsx'
        #self.dataframe = pd.read_excel(sourceFile, sheet_name = 'TRANS_AND_SAVINGS_ACCOUNTS')

        #self.validationChecklist = 'F:/CUA OpenBank API/OpenBanking/Integrated-v-1-4/Data/validation_checklist.txt'
        self.valid = '1'
        
        # Adding this as part of skipping duplicate product check.
        # to turn duplicate check on, make this indicator to 1
        self.productRepeteCheck = 0
        
        self.validationChecklist = validationChecklist
        exportpath = exportpath + '/ErrorList.txt'
        sheets = ['CRED_AND_CHRG_CARDS', 'TRANS_AND_SAVINGS_ACCOUNTS', 'TERM_DEPOSITS']
        self.healthreport = []
        self.product = []
        self.lastupdated = []
        for sheet in sheets:
            self.dataframe = pd.read_excel(sourceFile, sheet_name = sheet)
            dfCheck = self.dataframe.iloc[:,2:]
            dfCheck = dfCheck.dropna()
            if dfCheck.empty:
                #self.healthreport.append("/*"*10 + sheet + " is empty.")
                continue
            self.healthreport.append('*' * 100)
            self.healthreport.append(sheet)
            self.healthreport.append('*' * 100)
            self.main(sheet)
        
        if log:
            conn = Connector(root, '', log)
            conn.openConnector()
            self.healthreport.append('*' * 100)
            self.healthreport.append("*"*30 + "Product ID with provided updated date already exist")
            self.healthreport.append('*' * 100)
            self.DBProd = conn.executeQuery('select distinct productid, lastupdated from product')
            self.DBProd = [[x[0], x[1]] for x in self.DBProd]
            # self.DBLastUpd = [x[1] for x in self.DBProd]
            conn.closeConnector()
        
        i = 0
        for productID in self.product:
            for row in self.DBProd:
                if productID == row[0] and self.lastupdated[i] == row[1] and self.productRepeteCheck == 1:
                    self.healthreport.append(productID + " - already exists")
                    self.valid = '0'
            i += 1
        
        if len(self.product) != len(set(self.product)) and self.productRepeteCheck == 1:
            self.healthreport.append("*"*10 + "Product ID is repeted in the provided sheet")
            self.valid = '0'
        
        if(len(self.healthreport) > 1):
            self.writeHealth(exportpath)
        else:
            self.valid = '1'
        
            
    def getFeilds(self, header):
    
        mandatory = {}
        with open(self.validationChecklist, 'r') as f:
            begin = 0
            for line in f:
                if line.strip() == header:
                    begin = 1
                    
                elif line.strip() == '':
                        begin = 0
                
                elif begin == 1:
                    key_value = line.split(":")
                    mandatory.update({key_value[0].strip()[1:-1] : key_value[1].strip()})
        return mandatory
    
    def getFeildAttributes_mandatory(self, dataframe, feildList):
        
        attributeDictionaryList = []
        
        for feild in feildList.keys():
            attributeDictionary = {}            
            
            #feild = 'productCategory (Mandatory)'
            try:
                df = dataframe.loc[dataframe['Product'] == feild]
            except:
                self.healthreport.append("Mandatory feild is missing - " + feild)
                self.valid = '0'
                return 1
            
            attributeDictionary.update({'feildName' : feild})
            values = df.iloc[:,2:].values.tolist()[0]
            if feild == 'Product ID (Mandatory)':
                self.product += values
            elif feild == 'CUA Effective From (Mandatory) Date/Time':
                self.lastupdated += values
            
            attributeDictionary.update({'values' : values})
            attributeDictionary.update({'indexLength' : len(df)})
            if 'date' in feild.lower():
                #date check
                if True in [type(x) is not datetime.datetime for x in values]:
                    characterBreach = True
                    
                else:
                    characterBreach = False
            else:
                try:
                    characterBreach = any(len(i) > int(feildList[feild]) for i in values)
                    
                except TypeError:
                    characterBreach = False
                
            attributeDictionary.update({'CharacterBreach' : characterBreach})
            attributeDictionaryList.append(attributeDictionary)
    
        return attributeDictionaryList
    
    def writeHealth(self, file):
        with open(file, 'w') as f:
            for line in self.healthreport:
                f.write(str(line) + '\n')
    
    def gethealthreport_mandatoryFeilds(self, getFeildAttributesList):
        report = []
        mandatoryValuesCount = None
        
        for item in getFeildAttributesList:
            if item['CharacterBreach']:
                report.append("Characters breached in feild : " + item['feildName'])
                self.valid = '0'
            if item['indexLength'] > 1:
                report.append("Multiple mandatory feild found, check template for feild : " + item['feildName'])
                self.valid = '0'
            if mandatoryValuesCount is not None and len(item['values']) != mandatoryValuesCount or np.nan in item['values']:
                report.append("Values in mandatory feild missing, check feild in Product section : " + item['feildName'])
                self.valid = '0'
            else:
                mandatoryValuesCount = len(item['values'])
                
        return report
    
    
    def checkNonMandatory(self, dataframe, header):
    #    header = 'Features'
        feature = self.getFeilds(header)
        df = self.dataframe.loc[self.dataframe['Product'] == list(feature.keys())[0]]
        index = df.index.tolist()
        try:
            indexes = index[1] - index[0]
        except:
            indexes = len(feature.keys())
        
        # Checking index difference should be same
        y = 0
        for i in range(len(index)-1):
            if (index[i+1] - index[i]) != indexes:
                self.healthreport.append("Check template, additional/missing fields in : " + header)
                self.valid = '0'
        
        # if no of fields is more than index difference -> issue
        columns = len(feature.keys())
        if indexes < columns:
            self.healthreport.append("Unexpected error in section (more columns than Index): " + header)
            self.valid = '0'
        
        dict = {}
        for i in feature.keys():
            dict.update({i : []})
        
        # Extract data from 
        for idx in index:
            for i in range(columns):
                j = i+idx
                columnValues = self.dataframe.iloc[:,2:].loc[j].tolist()
                key = list(feature.keys())[i]
                val = dict[key]
                if True in [len(str(x)) > int(list(feature.values())[i]) and x != np.nan for x in val]:
                    self.healthreport.append("Character length breached in : " + key)
                    self.valid = '0'
                dict[key] = val + columnValues
        
        listOfAccumulatedvalues = list(dict.values())
        
        numberOfCols = len(listOfAccumulatedvalues[0])
            
        
        for j in range(numberOfCols):
            dataSet = []
            for key in dict.keys():
                dataSet.append(dict[key][j])
            
            if dataSet[0] == np.nan:
                len(set(dataSet)) > 1
                self.healthreport.append(list(feature.keys())[0] + " : ia a madatory feild and is required")
                self.valid = '0'
        
    def main(self, sheet):

        header = 'Mandatory Fields'
        mandatory = self.getFeilds(header)
        
        
        getFeildAttributesList = self.getFeildAttributes_mandatory(self.dataframe, mandatory)
        
        self.healthreport.append(self.gethealthreport_mandatoryFeilds(getFeildAttributesList))
        
        if sheet == 'TRANS_AND_SAVINGS_ACCOUNTS':
            headerList = ['Features', 'Constraints', 'Eligibility', 'Fee', 'Depositrate'] 
            [self.checkNonMandatory(self.dataframe, x) for x in headerList]
            print('done')
        elif sheet == 'CRED_AND_CHRG_CARDS':
            headerList = ['Features', 'Constraints', 'Eligibility', 'Fee', 'Lendingrate']  
            [self.checkNonMandatory(self.dataframe, x) for x in headerList]
            print('done')

        elif sheet == 'TERM_DEPOSITS':
            headerList = ['Features', 'Constraints', 'Eligibility', 'Fee']  
            [self.checkNonMandatory(self.dataframe, x) for x in headerList]
            print('done')
       
        
        
        
        
'''
sourceFile = 'F:/CUA OpenBank API/OpenBanking/Integrated-v-1-4/Data/product_file.xlsx'
validationChecklist = 'F:/CUA OpenBank API/OpenBanking/Integrated-v-1-4/Data/validation_checklist.txt'
exportPath = 'F:/CUA OpenBank API/OpenBanking/Integrated-v-1-4/ExportDF'
v = ValidationCheck(sourceFile, validationChecklist, exportPath, root, log)
v.healthreport
v.product
v.lastupdated
v.DBProd

df = v.dataframe

header = 'Eligibility'

feature = v.getFeilds(header)

df = v.dataframe.loc[v.dataframe['Product'] == list(feature.keys())[0]]

'''