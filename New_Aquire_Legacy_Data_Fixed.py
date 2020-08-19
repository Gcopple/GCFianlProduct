import os
import sys
import sqlite3
import datetime
from sqlite3 import Error

#########################################################################################################################################################################################################################################
###                                                                                                      Database Function                                                                                                            ###
#########################################################################################################################################################################################################################################

create_job_table = """
CREATE TABLE IF NOT EXISTS Job (
Project_Name TEXT,
Job_Name TEXT,
Number_of_Flight_Lines INTEGER,
Linear_Miles REAL,
Total_Processing_Time TEXT,
Number_of_Inputs INTEGER,
Number_of_Outputs INTEGER,
Size_GB_In REAL,
Size_GB_Out REAL,
Point_Density TEXT,
Path TEXT,
CONSTRAINT Job_ID PRIMARY KEY (Job_Name)
);
"""

create_line_table = """
CREATE TABLE IF NOT EXISTS Line (
Job_Name TEXT, 
Line_Name TEXT,
Number_Of_Input_Files INTEGER,
Number_Of_Output_Files INTEGER,
Input_Size_GB REAL,
Output_Size_GB REAL,
Total_Processing_Time_Seconds REAL,
Total_Processing_Time TEXT,
Point_Density TEXT,
Region TEXT,
CONSTRAINT Line_ID PRIMARY KEY (Job_Name, Line_Name)
);
"""

def updateLineTable(cursor, con, data):

    try:

        print("Trying To Update")
        data = (data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[0], data[1])
        lineUpdateQuery = """UPDATE Line SET Number_Of_Input_Files = ?, Number_Of_Output_Files = ?, Input_Size_GB = ?, Output_Size_GB = ?, Total_Processing_Time_Seconds = ?, Total_Processing_Time = ?, Point_Density = ?, Region = ? WHERE Job_Name = ? AND  Line_Name = ?"""
        cursor.execute(lineUpdateQuery, data)
        con.commit()
        print("Record Updated successfully")

    except Error as e:
        print(f"The error '{e}' has occurred")

def FORMAT_FOR_LINE(con, data):    

    insertIntoLine = """INSERT INTO Line
                          (Job_Name, Line_Name, Number_Of_Input_Files, Number_Of_Output_Files, Input_Size_GB, Output_Size_GB, Total_Processing_Time_Seconds, Total_Processing_Time, Point_Density, Region) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    cursor = con.cursor()

    try:

        cursor.execute(insertIntoLine, data)
        con.commit()

    except Error as e:

        print(f"The error '{e}' has occurred")
        updateLineTable(cursor, con, data)
        
def updateJobTable(cursor, con, data):

    try:

        print("Trying To Update")
        data = (data[0], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[1])
        jobUpdateQuery = """UPDATE Job SET Project_Name = ?, Number_of_Flight_Lines = ?, Linear_Miles = ?, Total_Processing_Time = ?, Number_of_Inputs = ?, Number_of_Outputs = ?, Size_GB_In = ?, Size_GB_Out = ?, Point_Density = ?, Path = ? WHERE Job_Name = ?"""
        cursor.execute(jobUpdateQuery, data)
        con.commit()
        print("Record Updated successfully")

    except Error as e:
        print(f"The error '{e}' has occurred")

def FORMAT_FOR_JOB(con, data):

    insertIntoJob = """INSERT INTO Job
                         (Project_Name, Job_Name, Number_of_Flight_Lines, Linear_Miles, Total_Processing_Time, Number_of_Inputs, Number_of_Outputs, Size_GB_In, Size_GB_Out, Point_Density, Path)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    cursor = con.cursor()
    
    try:
        
        cursor.execute(insertIntoJob, data)
        con.commit()

    except Error as e:

        print(f"The error '{e}' has occurred")
        updateJobTable(cursor, con, data)

def Create_Connection(path):

    connection = None

    try:

        connection = sqlite3.connect(path)

        print("Connection to SQLite DB successful")

    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def Execute_Query(connection, query):

    cursor = connection.cursor()

    try:

        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")

    except Error as e:
        print(f"The error '{e}' occurred")

def Execute_Read_Query(connection, query):

    cursor = connection.cursor()
    result = None

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result

    except Error as e:
        print(f"The error '{e}' occurred")

#########################################################################################################################################################################################################################################

def Find_Lifts(home):
    listOfLifts = list()

    for projectName in os.listdir(home):

        projectDirectory = os.path.join(home,projectName)

        if ("ITC" not in projectName) and ("Ortho_Lidar" not in projectName) and os.path.isdir(projectDirectory):

            for Acquisition in os.listdir(projectDirectory):

                if Acquisition == "03_Acquisition":

                    acquisitionDirectory = os.path.join(projectDirectory, Acquisition)

                    for toBeNamedLater in os.listdir(acquisitionDirectory):

                        directoryToBeNamedLater = os.path.join(acquisitionDirectory, toBeNamedLater)

                        if os.path.isdir(directoryToBeNamedLater) and ("N" in toBeNamedLater[0]):

                            for lift in os.listdir(directoryToBeNamedLater):

                                liftDirectory = os.path.join(directoryToBeNamedLater,lift)
                                
                                if ("Base" not in lift) and ("Flood" not in lift) and (".BIN" not in lift)and os.path.isdir(liftDirectory):

                                    for info in os.listdir(liftDirectory):

                                        if os.path.isdir(os.path.join(liftDirectory,info,"logs")) and (os.path.isdir(os.path.join(liftDirectory,info,"intermediate")) == False):
                                            
                                            listOfLifts.append(liftDirectory)
    return listOfLifts
            

#########################################################################################################################################################################################################################################
def Get_Input(home):
    
    missionList = list()
    QL_List = list()
    lineSpreadOutLength = list()
    counter = 0
    inputSize = 0


    for i in ["MM1","MM2","MM3","MM4"]:

        MMDir = os.path.join(home,i)

        for j in os.listdir(MMDir):

            possibleMission = os.path.join(MMDir,j)

            if os.path.isdir(possibleMission) & (j != "Calibration") & (j != "LogFiles") & ("Frames" not in j):
                mission = possibleMission

                if mission not in missionList:
                    missionList.append(mission)
                    
                for k in missionList:

                    if k not in QL_List:
                        QL_List.append(k)

                        for l in os.listdir(k):

                            regionList = os.path.join(k,l)

                            for ppm in l.split("_"):
                                if "ppm" in ppm:
                                    numberOfPPM = ppm

                            for lineName in os.listdir(regionList):

                                inputFilePath = os.path.join(regionList,lineName)

                                for inputFileName in os.listdir(inputFilePath):

                                    if "T0_Returns.dat" in inputFileName[-14:]:
                                        counter += 1

                                        inputSize += os.path.getsize(os.path.join(inputFilePath,inputFileName))

                                lineSpreadOutLength.append((lineName,counter,numberOfPPM,inputSize))
                                counter = 0
                                inputSize = 0

    lineInputInformation = list()
    orderedLineNames = list()

    for i in lineSpreadOutLength:

        length = 0
        sizeOfInputFiles = 0

        for j in lineSpreadOutLength:

            if i[0] == j[0]:

                length += j[1]
                sizeOfInputFiles += j[3]

        if i[0] not in orderedLineNames:

            orderedLineNames.append(i[0])
            lineInputInformation.append((i[0], length, i[2], round(sizeOfInputFiles/(1024*1024*1024), 3)))

    return lineInputInformation

def Get_Path_To_Output(home):

    logPathList = list()
    jobPathList= list()

    for i in os.listdir(home):
        logPath = os.path.join(os.path.join(home,i),"logs")

        if os.path.isdir(logPath):
            logPathList.append(logPath)
            jobPathList.append(os.path.join(home,i))

    return logPathList, jobPathList




#########################################################################################################################################################################################################################################
###                                                                                                      Log Functions                                                                                                                ###
#########################################################################################################################################################################################################################################

#This function goes inside of our job tracking and will give us the times.
def Calculate_Exact_Run_Time(lines, line):

    #Get The Time Unformated Time String
    firstLine = lines[0]
    lastLine = line

    #Parse The Time
    startDateTime = firstLine.split(",")[0]
    endDateTime = lastLine.split(",")[0]
    startTime = startDateTime.split(" ")[1]
    endTime = endDateTime.split(" ")[1]
    startDate = startDateTime.split(" ")[0]
    endDate = endDateTime.split(" ")[0]
    startDateList = startDate.split("-")
    endDateList = endDate.split("-")
    startTimeList = startTime.split(":")
    endTimeList = endTime.split(":")

    #Convert The Time To Seconds
    startHours = int(startTime[:2])*3600
    startMinutes = int(startTime[3:5])*60
    startSeconds = int(startTime[6:8])
    endHours = int(endTime[:2])*3600
    endMinutes = int(endTime[3:5])*60
    endSeconds = int(endTime[6:8])

    #Make A Time Object of Format HH:MM:SS
    s = datetime.datetime(int(startDateList[0]),int(startDateList[1]),int(startDateList[2]),int(startTimeList[0]),int(startTimeList[1]),int(startTimeList[2]))
    e = datetime.datetime(int(endDateList[0]),int(endDateList[1]),int(endDateList[2]),int(endTimeList[0]),int(endTimeList[1]),int(endTimeList[2]))

    timeDelta = str(e - s)

    return timeDelta, s, e

def Log_Parsing_Thread(logName, inputListOfTuples, missionName, pathForDB):

    # Variables that I need to be local
    sForLater = None
    eForLater = None
    processName = None
    logIsFinished = False

    #This is the process to read in the log file until the "root - End ingest" is added to a line 

    with open(logName, 'r') as log:

        lines = log.readlines()

        for line in lines:

            if "root - End ingest" in line and ("Licensing initialization" not in lines[lines.index(line)-1]):

                timeInformation = Calculate_Exact_Run_Time(lines, line)

                if processName == "Create":

                    sForLater = timeInformation[1]
                    return (lineNumber, processName, timeInformation[0], missionName, pathForDB, sForLater)

                elif processName == "Georeferencing":
                     return (lineNumber, processName, missionName, timeInformation[0])

                elif processName == "Process Pointcloud":
                    return (lineNumber, processName, missionName, timeInformation[0])


                elif processName == "Filter Pointcloud":
                    return (lineNumber, processName, missionName, timeInformation[0])

                elif processName == "Update Las Reference":
                    return (lineNumber, processName, missionName,timeInformation[0])

                elif processName == "Update Mean Terrain Height":

                    eForLater = timeInformation[2]
                    return (lineNumber, processName, timeInformation[0], missionName, pathForDB, eForLater)
                    
                else:
                    lock.release()


            elif "_create_.rsp" in line:

                processName = "Create"
                lineNumber = "NA"

            elif "_ahab_georeferencing_" in line:

                processName = "Georeferencing"
                lineNumberIndex = line.split("_").index("georeferencing") + 1
                lineNumber = line.split("_")[lineNumberIndex] + "_" +line.split("_")[lineNumberIndex+1][:6]


            elif "_process_pointcloud_" in line:

                processName = "Process Pointcloud"
                lineNumberIndex = line.split("_").index("pointcloud") + 1
                lineNumber = line.split("_")[lineNumberIndex] + "_" +line.split("_")[lineNumberIndex+1][:6]

                for inputInformation in inputListOfTuples:
                    if lineNumber == inputInformation[0]:
                        numberOfInputFiles = inputInformation[1]

            elif "_ahab_filter_pointcloud_" in line:

                processName ="Filter Pointcloud"
                lineNumberIndex = line.split("_").index("pointcloud") + 1
                lineNumber = line.split("_")[lineNumberIndex] + "_" +line.split("_")[lineNumberIndex+1][:6]
                        
            elif "_ahab_updateLasReference_" in line:

                processName ="Update Las Reference"
                lineNumber = "NA"

            #This is the final process
            elif "_updateMeanTerrainHeight_" in line:

                processName = "Update Mean Terrain Height"
                lineNumber = "NA"
                
                            
    log.close()


def Job_Thread_Manager(logPathList, inputTupleList):
    listFromLogs = list()
    for logFolder in logPathList:

        pathName = logFolder.split("\\")[-2]
        pathForDB = logFolder

        for logName in os.listdir(logFolder):

            logPath = os.path.join(logFolder,logName)
            listFromLogs.append(Log_Parsing_Thread(logPath, inputTupleList, pathName, pathForDB))
    return listFromLogs 

#########################################################################################################################################################################################################################################
###                                                                                                   Format Functions                                                                                                                ###
#########################################################################################################################################################################################################################################


def Format_Time(timeString):
    timeList = timeString.split(":")
    timeInSeconds = int(timeList[0])*3600 + int(timeList[1])*60 +int(timeList[2])
    return timeInSeconds

def Format_Information_For_DB(inputInfo, trackingInfo, outputInfo):
    condensedTrackingInfo = list()
    lineNameList = list()
    condensedJobInfo = list()

    for i in trackingInfo:

        processingTime = 0

        for j in trackingInfo:

            if (i[0] == j[0]) and (i[0] != 'NA') and (i[2] == j[2]) and ((i[0],i[2]) not in lineNameList):
                processingTime += Format_Time(j[3])               

            elif (i[0] == j[0]) and (i[1] == 'Create') and (j[1] == 'Update Mean Terrain Height') and (i[3] == j[3]):
                totalTime = str(j[5] - i[5]) 
                condensedJobInfo.append((i[3], totalTime, j[4]))

        if (i[0] != 'NA') and ((i[0],i[2]) not in lineNameList):
            lineNameList.append((i[0],i[2]))
            condensedTrackingInfo.append((i[0], processingTime, i[2]))

    lineDatabaseInfo = list()

    for i in inputInfo:

        for j in outputInfo:

            for k in condensedTrackingInfo:

                if(i[0] == j[0] == k[0]):

                    fullLineInfo = (k[2], i[0], i[1], j[1], i[3], j[2], k[1], str(datetime.timedelta(seconds=k[1])), i[2], j[3])
                    lineDatabaseInfo.append(fullLineInfo)

    jobDatabaseInfo = list()
    fixerror = list()
    
    for i in condensedJobInfo:

        totalNumberOfLines = 0
        totalInputSize = 0
        totalOutputSize = 0
        totalInputFiles = 0
        totalOutputFiles = 0

        for j in lineDatabaseInfo:

            if i[0] == j[0] and ((j[0],j[1]) not in fixerror):

                fixerror.append((j[0],j[1]))
                totalInputFiles += j[2]
                totalOutputFiles += j[3]
                totalNumberOfLines += 1
                totalInputSize += j[4]
                totalOutputSize += j[5]
                ppm = j[8]
                
        projectName = i[2].split("\\")[4]
        fakeLinearMiles = round((totalOutputFiles - totalNumberOfLines) * 0.621371, 2)
        fullJobInfo = (projectName, i[0], totalNumberOfLines, fakeLinearMiles, i[1], totalInputFiles, totalOutputFiles, totalInputSize, totalOutputSize, ppm, i[2])
        jobDatabaseInfo.append(fullJobInfo)
    
    return lineDatabaseInfo, jobDatabaseInfo

#########################################################################################################################################################################################################################################
#########################################################################################################################################################################################################################################
###                                                                                                   Output Functions                                                                                                                ###
#########################################################################################################################################################################################################################################


def Get_Output_Info(pathsToJobs):

    outputList = list()

    for pathToJob in pathsToJobs:

        for i in os.listdir(pathToJob):

            pathToOutput = os.path.join(pathToJob,i)

            if os.path.isdir(pathToOutput) and i != "cam" and i != "logs" and i != "gps-imu" and i !="Junk" and "All_Points" not in i and i != "intermediate":

                for j in os.listdir(pathToOutput):

                    lineContainer = os.path.join(pathToOutput,j)

                    if "TM" in lineContainer:

                        missionName = lineContainer.split("\\")[-1]

                        for line in os.listdir(lineContainer):

                            outputSize = 0
                            lasOutputFilesDir = os.path.join(os.path.join(lineContainer,line),"pc")
                            lineName = line
                            listOfLasOutputFiles = os.listdir(lasOutputFilesDir)
                            numberOfOutputFiles = len(listOfLasOutputFiles)

                            for lasOutputFile in listOfLasOutputFiles:

                                outputSize += os.path.getsize(os.path.join(lasOutputFilesDir, lasOutputFile))
                                
                            outputList.append((lineName, numberOfOutputFiles, round(outputSize/(1024*1024*1024), 3), missionName))

    return outputList

#########################################################################################################################################################################################################################################


def main():

    projectHomeDirectory = "\\\\Lidar04\\Vol1"
    conn = Create_Connection("\\\\Lidar01\\Vol1\\HxMap_Shared\\Database\\LegacyDataFinal.db")
    projectList = Find_Lifts(projectHomeDirectory)

    for project in projectList:
        inputInformation = Get_Input(project)
        pathsToOutput = Get_Path_To_Output(project)
        logInformation = Job_Thread_Manager(pathsToOutput[0], inputInformation)
        outputInformation = Get_Output_Info(pathsToOutput[1])
        formatedData = Format_Information_For_DB(inputInformation, logInformation, outputInformation)

        Execute_Query(conn, create_job_table)
        Execute_Query(conn, create_line_table)

        for lineData in formatedData[0]:
            FORMAT_FOR_LINE(conn,lineData)

        for jobData in formatedData[1]:
            FORMAT_FOR_JOB(conn,jobData)
    
    conn.close()

    
if __name__ == "__main__":
    main()
    

