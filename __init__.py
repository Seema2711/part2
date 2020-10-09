import configparser
import os
import sys

# Import DOP class
from DataOperations import DOP

# Log initiation -
from RunLog import RunLog
from validation import ValidationCheck
# Copy file from s3 bucket to Data folder
import subprocess
from datetime import datetime

# Class to Process rates Sheet
from Rates import splitProduct as SP
# Set Root folder
root = os.path.dirname(os.path.realpath(__file__))

# Read Configuration file
config = configparser.ConfigParser()
config.read('CONFIG.INI')

dynamicFileName = str(datetime.now()).replace(" ",'-').replace(':','')[:15] + "-File.xlsx"



#dataFile = root + "/Data/" + dynamicFileName
dataFile = root + "/Data/" + config["DEFAULT"]["FileName"]
validationChecklist = root + "/Data/" + config["DEFAULT"]["ValidationChecklist"]
validationEnabled = config["DEFAULT"]["ValidationEnables"]
errorFilePutCommand = "aws s3 cp ExportDF/ErrorList.txt s3://" + config["DEFAULT"]["S3Bucket"] + "/ErrorList.txt"
copyProductFileCommand = "aws s3 cp s3://" + config["DEFAULT"]["S3Bucket"] + "/ProductFileFinal7777.xlsx " + dataFile
# "aws s3 cp s3://" + config["DEFAULT"]["S3Bucket"] + "/ProductFileFinal7777.xlsx Data/product_file.xlsx"


archivePath = root + config["DEFAULT"]["ArchivePath"]
exportPath = root + config["DEFAULT"]["ExportPath"]

subprocess.call(copyProductFileCommand, stdout=subprocess.PIPE, shell=True)

log = RunLog(archivePath, exportPath)

v = ValidationCheck(dataFile, validationChecklist, exportPath, root, log)
if validationEnabled.lower() == 'y':
    if v.valid == '0':
        subprocess.call(errorFilePutCommand, stdout=subprocess.PIPE, shell=True)
        sys.exit(0)

def extractData(inSheet):        
    
    DOPobject = DOP()
    
    DOP.path = exportPath + "/"
    DOP.log = log
    log.logComment("DOP Object created and Path set to - " + DOP.path)
    
    # Read third sheet
    df = DOPobject.readXLS(path = dataFile, sheet = inSheet)
    
    # Output dictionary stored here are the row number of different child tables
    childTablePosition = DOPobject.splitBoundaries(df)
    
    def splitTable(from_position, to_position = None):
        if(to_position != None):
            return DOPobject.splitRootTable(df, childTablePosition[from_position], \
                                            childTablePosition[to_position])
        else:
            return DOPobject.splitRootTable(df, childTablePosition[from_position])
    
    # Define Split sequence
    
    productDF = splitTable('Product', 'Card Art')
    
    log.logComment(childTablePosition)
    
    if childTablePosition['Eligibility'] > childTablePosition['Constraints'] :
        log.logComment("Following sequence Product, Features, Constraint, Eligibility, Fees, Dep/Len")
        # Ading for Card
        cardDF = splitTable('Card Art', 'Features')
        featureDF = splitTable('Features', 'Constraints')
        constraintsDF = splitTable('Constraints', 'Eligibility')
        eligibilityDF = splitTable('Eligibility', 'Fees')
    
    
    elif childTablePosition['Eligibility'] < childTablePosition['Constraints']:
        log.logComment("Following sequence Product, Features, Eligibility, Constraint, Fees, Dep/Len")
        # Ading for Card
        cardDF = splitTable('Card Art', 'Features')
        featureDF = splitTable('Features', 'Eligibility')
        constraintsDF = splitTable('Constraints', 'Fees')
        eligibilityDF = splitTable('Eligibility', 'Constraints')
    
    if('Deposit Rates' in childTablePosition.keys()):
        log.logComment("Found deposit rates tab")
        feesDF = splitTable('Fees', 'Deposit Rates')
        depositRateDF = splitTable('Deposit Rates')
        
    elif('Lending Rates' in childTablePosition.keys()):
        log.logComment("Found lending rates tab")
        feesDF = splitTable('Fees', 'Lending Rates')
        depositRateDF = splitTable('Lending Rates')
        
    # Extract and save files
    nameOfoutFile = {
            'TRANS_AND_SAVINGS_ACCOUNTS' : "TnS",
            'CRED_AND_CHRG_CARDS' : "CCC",
            'TERM_DEPOSITS' : "TeD"
        }
    
    name = nameOfoutFile[inSheet]
    nameOut = (name + "_Card")
    DOPobject.separateChildContent(cardDF, "Card Type", ret = 0, name = nameOut)
    
    name = nameOfoutFile[inSheet]
    nameOut = (name + "_Feature")
    DOPobject.separateChildContent(featureDF, "Feature Type", ret = 0, name = nameOut)
    
    nameOut = (name + "_Rate")
    DOPobject.separateChildContent(depositRateDF, "Rate Type (Mandatory)", ret = 0, name = nameOut)
    
    nameOut = (name + "_Fees")
    DOPobject.separateChildContent(feesDF, "Fee Name", ret = 0, name = nameOut)
    
    nameOut = (name + "_Eligibility")
    DOPobject.separateChildContent(eligibilityDF, "Eligibility Type", ret = 0, name = nameOut)
    
    nameOut = (name + "_Constraints")
    DOPobject.separateChildContent(constraintsDF, "Constraint Type", ret = 0, name = nameOut)
    
    nameOut = (name + "_Products")
    DOPobject.processSingleDataset(productDF, nameOut)


from FileOperations import FileOperations
expPath = root + config["DEFAULT"]["ExportPath"]

#f = FileOperations("E:/CUA OpenBank API/OpenBanking/DataProcesing")
f = FileOperations(root)
i = 0

#for sheet in [3,4,5,6]:
sheets = ['TRANS_AND_SAVINGS_ACCOUNTS', 'CRED_AND_CHRG_CARDS', 'TERM_DEPOSITS',
         'TERM_DEPOSITS_RATES']

for sheet in sheets:
    if sheet == 'TERM_DEPOSITS_RATES':
        rates = SP(dataFile, sheet)
        if not rates.df.iloc[:,2:].empty:
            rates.log = log
            rates.path = exportPath + "/"
            rates.createDict()
        
    else:
        log.logComment("Parsing sheet number " + str(sheet))
        extractData(sheet)
        log.logComment("Success: Stopping ")
   
from main1 import ETL


RunByUserName = config["DEFAULT"]["RunByUserName"]
archivePath = archivePath
dataPath = exportPath
        

ETL(RunByUserName, archivePath, dataPath, root)

dataRejectLog = []
writeLog = 0
confirmReject = 0

with open(log.logPath, 'r') as f:
    for line in f:
        if " -> Connection Status: 0" in line:
            dataRejectLog.append(line)
        elif 'Insert reject data:' in line:
            writeLog = 1
        elif writeLog == 1 and line[25] == '[' and len(line) > 28:
            confirmReject  = 1
            dataRejectLog.append(line)
        
if confirmReject == 1:
    path = exportPath + "/" + 'RejectLog.txt'
    with open(path, 'w') as f:
        for l in dataRejectLog:
            f.write(l)

    