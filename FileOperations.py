import os
from DataOperations import DOP

class FileOperations:
    
    def __init__(self, root):
        exportFolder = root + "/ExportDF"
        
        self.pd = DOP()
        
        outFiles = ['Constraints.csv', 'Eligibility.csv', 'Feature.csv', \
                    'Fees.csv', 'Products.csv', 'Rate.csv']
        
        filenames = ["A.csv", "B.csv","C.csv","D.csv","E.csv", "F.csv"]
        
        self.fullPathFiles = []
        self.filename = []
        
        for file in outFiles:
            self.fullPathFiles.append(exportFolder + "/" + file)
        
        for file in filenames:
            self.filename.append(exportFolder + "/" + file)
            
    
    def rename(self, order = 0):
        
        i = 0
        for file in self.fullPathFiles:
            if(order == 0):
                os.rename(file,self.filename[i])
            elif(order == 1):
                os.remove(file)
                os.rename(self.filename[i], file)
            i+=1
    
    def mergeResults(self):
        i = 0
        for file in self.fullPathFiles:
            self.pd.mergeDF(self.filename[i], self.fullPathFiles[i])
            i+=1