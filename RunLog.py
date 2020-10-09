import glob
from datetime import date as dt
from datetime import datetime
import os
import shutil as sh


class RunLog:

    def __init__(self, ArchivePath, dataFolder, moveFiles = True):
        self.logPath = dataFolder + "/log.txt"
        
        date = dt.today()
        newFolderName = str(date.day) + str(date.month) + str(date.year)
        
        
        # Check if folder in archive exist
        folderSuffix = 0
        while newFolderName in os.listdir(ArchivePath):
            newFolderName = str(date.day) + str(date.month) + str(date.year) + "_" + str(folderSuffix + 1)
            folderSuffix += 1
        
        archiveFromLastRun = ArchivePath + "/" + newFolderName
        
        try:
            os.mkdir(archiveFromLastRun)
            self.logComment("Start")
        except:
            self.logComment("Failed to create archive at - " + archiveFromLastRun)
        else:
            self.logComment("Success to create archive at - " + archiveFromLastRun)
        
        if(moveFiles):
            # Move last run files to Archive
            for file in os.listdir(dataFolder):
                file = dataFolder + "/" + file
                try:
                    sh.move(file, archiveFromLastRun)
                    self.logComment("Success: File moved - " + file)
                except:
                    self.logComment("Failed: File moved - " + file)
        
        self.logComment("Archival completed to - " + archiveFromLastRun)
        
            
    def logComment(self, comment, withnoTS = False):
        comment = str(comment)
        
        if(withnoTS):
            comment = "\t\t\t " + comment + "\n"
            self.writeComment(comment)
            
        else:
            ts = datetime.now().strftime("%d-%b-%Y %H:%M:%S :   ")
            comment = ts + comment
            self.writeComment(comment)
            
    def writeComment(self, comment):
        comment += "\n"
        
        with open(self.logPath, "a") as f:
            f.write(comment)
            f.close()
'''
archivePath = "C:/Users/palas/Documents/Eclipse Projects/CUA OpenBank API/OpenBanking/DataProcesing/Archive"
dataFolder = "C:/Users/palas/Documents/Eclipse Projects/CUA OpenBank API/OpenBanking/DataProcesing/ExportDF"
r = RunLog(archivePath, dataFolder)
r.logComment("log with TS")
r.logComment("log with noTS", withnoTS = True)
'''