import pandas as pd
import numpy as np

class DOP:
    
    path = ""
    log = ""
    # Return Data frame from input excel file 
    
    def readXLS(self, path, sheet):
        df = pd.read_excel(path, sheet)
        print(df)
        # try:
        #     df = pd.read_excel(path, sheet_name = sheet)
        #     print(df)
        # except:
        #     self.log.logComment("Failed : Pandas can not be called")
        return df
    
    # Define boundaries for various data sets
    
    def splitBoundaries(self, df):
        
        self.log.logComment("fn: splitBoundaries - Start")
        
        dataLabels = {'Product' : 0,
                      'Card Art' : 0,
                      'Features': 0,
                      'Constraints' : 0,
                      'Eligibility' : 0,
                      'Fees' : 0,
                      'Deposit Rates' : 0,
                      'Lending Rates' : 0
                      }
        
        for Key,Val in dataLabels.items():
            
            try:
                rowNumber = df.loc[df["Product"]== Key].index[0]
                
            except IndexError:
                rowNumber = 0
        
            dataLabels.update({Key : rowNumber})
        
        deleteKey = ""
        for key in dataLabels.keys():
            if(key != 'Product' and dataLabels[key] == 0):
                deleteKey = key
        
        if(deleteKey != ""):
            del(dataLabels[deleteKey])
        
        self.log.logComment("fn: splitBoundaries - finish")
        
        return dataLabels
    
    # Split root table
    
    def splitRootTable(self, df, begin, end = None):
        df = df.iloc[begin:end,:]
        
        self.log.logComment("fn: splitRootTable - start" + str(begin) + " - " + str(end))
        
        # Store Product details in global variable. This can be concat to identify product
        
        if(begin == 0):
            self.ProdDF = df.loc[(df['Product']=="Product ID (Mandatory)") | \
                                        (df['Product']=="CUA Effective From (Mandatory) Date/Time")]
        
        else:
            df = pd.concat([df, self.ProdDF])
        self.log.logComment("fn: splitRootTable - finish")
        return df
    
    
    def separateChildContent(self, featureDF, first_Col, featureRows = 5, ret = 1, name = "DF"):
        
        self.log.logComment("fn: separateChildContent - Start")
        
        indexes = featureDF.loc[featureDF["Product"] == first_Col].index
        
        self.log.logComment("indexes = " + str(indexes))
        
        self.log.logComment(indexes)
        
        if(len(indexes) == 0):
            return 0
        
        begin = 0
        
        if(len(indexes) >= 2):
            featureRows = indexes[1] - indexes[0] 
            
        for v in indexes:
            if first_Col in ['Card Type', 'Eligibility Type', 'Feature Type','Rate Type (Mandatory)','Constraint Type', 'Fee Name']:
                features = featureDF.iloc[begin +1 : begin + featureRows +1,:]
            else:
                features = featureDF.iloc[begin : begin + featureRows,:]
                
            features = pd.concat([features, self.ProdDF])
            
            features = features.dropna(subset = ['Product'])
            
            ## On forst itteration table should initialize and on successive itterations it should update only
            if(begin == 0):
                table = Table(features, self.log)
            
            else:
                table.callUpdatetable(features)
            
            begin = begin + featureRows
            
        if(ret == 1):
            return table.getTable()
        elif(ret == 0):
            d = table.getTable()
            self.exportCSV(d, name)
            
        self.log.logComment("fn: separateChildContent - finish")
        
    
    def processSingleDataset(self, df, name):
        
        self.log.logComment("fn: processSingleDataset  - start; Init - Table Class")
        x = Table(df, self.log)
        if(np.nan in x.dict.keys()):
            del x.dict[np.nan]
        y = pd.DataFrame(x.dict)
        y = y.dropna(how = 'all')
        self.exportCSV(y, name)
        self.log.logComment("fn: processSingleDataset  - finish")
    
    def exportCSV(self, df, name):
        self.log.logComment("fn: exportCSV  - start")  
        #path = self.path + "/" + name + ".csv"
        path = self.path + name + ".csv"
        #df.to_csv(path)
        self.createCSV(df, path)
        self.log.logComment("fn: exportCSV  - finish")
    
    def stringConverter(self, column):
        columnStr = ""
        for key in column:
            #columnStr = columnStr + '"'+ str(key) + '",'
            # CHG- To Pipeline
            columnStr = columnStr + '"'+ str(key) + '"|'
        return columnStr[:-1]
    
    def createCSV(self, df, path):
        
        # Removing Null Product ID rows, This will make issue for two different card type
        
        df = df.dropna( subset = ['Product ID (Mandatory)'] )
        
        # Creating Debug Catch point
        try:
            column = df.to_dict().keys()
        except:
            "OK"
        rows = df.values.tolist()
        fileCSV = []
        fileCSV.append(self.stringConverter(column))
        for line in rows:
            fileCSV.append(self.stringConverter(line))
        
        with open(path, "w") as f:
            for line in fileCSV:
                f.write(line.replace('\n', ' ') + '\n')
    
    
    def mergeDF(self, path1, path2):
        
        self.log.logComment("fn: mergeDF  - start")
        DF1 = pd.read_csv(path1)
        DF2 = pd.read_csv(path2)
        DF = pd.concat([DF1, DF2])
        DF.to_csv(path1)
        self.log.logComment("fn: mergeDF  - finish")
# -------------------------------- Table Class---------------------------------#

# Class to make data table object
# Initialization require data frame
# {class object}.callUpdatetable(df) will append more rows to the table


class Table:
    
    def __init__(self,df, log):
        
        # Reverted
        # Dictionary to DF conversion change - past - arrayVal = df.iloc[:,1]
        
        arrayVal = df.iloc[:,1]
        self.dict = {}
        self.log = log
        
        self.log.logComment("Class: Table init - ")
        self.log.logComment(arrayVal)
        
        for i in arrayVal:
            self.dict.update({i:[]})
        #print("Dictionary creted as -")
        #print(self.dict)
        
        self.callUpdatetable(df)
            
            
    def updateTable(self, arrayDF):
        
        # Reverted : 
        #Dictionary to DF conversion change - past - arrayDimX = 0
        
        #self.log.logComment("fn - updateTable - start")
        
        arrayDimX = 0
        dfLength = len(arrayDF)
        
        #print("Recieved dataset")
        #print(arrayDF)
        
        while arrayDimX < dfLength:
            
            # Fix for key difference
            '''
            keyDifferences = {
                    'Feetype' : 'Fee type',
                    'Accrualfrequency' : 'Accrual Frequency',
                    'Additional Value (Auto Generated)' : 'Additionalvalue (AutoGenerated)'
                }
            
            if(arrayDF.iloc[arrayDimX, 0] in keyDifferences.keys()):
                key = keyDifferences[arrayDF.iloc[arrayDimX, 0]]
            
            else:
                key = arrayDF.iloc[arrayDimX, 0]
            
            '''
            
            key = arrayDF.iloc[arrayDimX, 0]
            
            # Check if key exist. If not Insert
            try:
                if(key != None):
                    preVal = self.dict[key]
                else:
                    continue
            
            # if a key is not found then a similar length of array to be added
            except KeyError:
                fullsizeKey = list(self.dict.keys())[1]
                size = len(self.dict[fullsizeKey])
                nullArray = [None] * size
                addArray = {key : nullArray}
                self.dict.update(addArray)
                arrayDimX += 1
                continue

            try:
                preVal.append(str(arrayDF.iloc[arrayDimX, 1]))
                
            except:
                preVal.append("2030-12-30 00:00:00")
                self.log.logComment("Warning - Date error, high date inserted by default")
                
                
            self.dict[key] = preVal
            arrayDimX += 1
        
        # find highest dimension in the dictionary
        maxLen = 0
        for key in self.dict.keys():
            if(maxLen < len(self.dict[key])):
                maxLen = len(self.dict[key])
        
        
        for key in self.dict.keys():
            if(maxLen > len(self.dict[key])):
                nullArray = [None] * (maxLen - len(self.dict[key]))
                val = self.dict[key]
                val += nullArray
                self.dict[key] = val
        
        #self.log.logComment(self.dict)
        #self.log.logComment("fn - updateTable - finish")
    
    def callUpdatetable(self, df):
        cols = len(df.columns)
        for i in range(2, cols):
            self.updateTable(df.iloc[:,[1, i]])
            
            
    def getTable(self):
        first = 0
        
        '''
        dict_df = {}
         
        for key, val in self.dict.items():
            if(first != 0):
                dict_df.update({key : val})
            else:
                first = 1
        #print(self.dict)
        print("*" * 20)
        
        # Break point
        
        try:
            dict_df = pd.DataFrame(dict_df)
        except:
            print(dict_df)
        '''
        dict_df = pd.DataFrame(self.dict)
        return dict_df
        #return self.dict
        
        
class splitProduct:
    
    def __init__(self, df, path):
        self.path = path
        self.df = df
        self.indexes = df.loc[df["Standard Term Deposits"] == "Product ID (Mandatory)"].index
    
    def createDict(self):
        subDF = self.df.iloc[:,self.indexes[0]:self.indexes[1]]
        print(subDF)    
        
#s = splitProduct()