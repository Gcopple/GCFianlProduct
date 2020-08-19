import os
import sys
import time
import threading
import datetime
import sqlite3
from sqlite3 import Error
from tqdm import tqdm
from time import sleep

#########################################################################################################################################################################################################################################
###                                                                                                   Input Functions                                                                                                                 ### 
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

    return lineInputInformation, orderedLineNames

def Get_Path_To_Output(home):

    logPathList = list()
    jobPathList= list()

    for i in os.listdir(home):
        logPath = os.path.join(os.path.join(home,i),"logs")

        if os.path.isdir(logPath):
            logPathList.append(logPath)
            jobPathList.append(os.path.join(home,i))

    return logPathList, jobPathList


##########################################################################################################################################################################################################################################
"""

"""
#########################################################################################################################################################################################################################################
###                                                                                                Threading Functions                                                                                                                ###
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

#This Function will be incharge of the tracking of the job. For now it also will be incharge of outputing the information for the database.
def Log_Parsing_Thread(logName, inputListOfTuples, loggingList, createLogThread, missionName, pathForDB, lock):

    # Variables that I need to be local
    sForLater = None
    eForLater = None
    processName = None
    currentLineIndex = 0
    numberOfLas = 0
    logIsFinished = False
    timeSlept = 0
    oldLength = 0

    #This is the process to read in the log file until the "root - End ingest" is added to a line 
    while not logIsFinished:

        newLength = os.path.getsize(logName)
        
        if newLength > oldLength:

            oldLength = newLength

            with open(logName, 'r') as log:

                try:
                    sleep(1)
                    lines = log.readlines()
                except:
                    while True:
                        sleep(1)
                        print(missionName + " is broken!")

                for line in lines[currentLineIndex:]:

                    currentLineIndex = len(lines) 

                    if "root - End ingest" in line and ("Licensing initialization" not in lines[lines.index(line)-1]):

                        logIsFinished = True
                        createLogThread = True

                        timeInformation = Calculate_Exact_Run_Time(lines, line)

                        if processName == "Create":

                            sForLater = timeInformation[1]
                            loggingList.append((lineNumber, processName, timeInformation[0], missionName, pathForDB, sForLater))
                            print("Created")



                        elif processName == "Georeferencing":
                            loggingList.append((lineNumber, processName, missionName, timeInformation[0]))
                            print("Georeferenced")



                        elif processName == "Process Pointcloud":
                            lock.acquire()
                            t.close()
                            loggingList.append((lineNumber, processName, missionName, timeInformation[0]))
                            lock.release()


                        elif processName == "Filter Pointcloud":
                            loggingList.append((lineNumber, processName, missionName, timeInformation[0]))
                            print("Flight Line " + lineNumber + " has completed with Filter Point Cloud")
                            
                            


                        elif processName == "Update Las Reference":
                            loggingList.append((lineNumber, processName, missionName,timeInformation[0]))
                            


                        elif processName == "Update Mean Terrain Height":

                            eForLater = timeInformation[2]
                            loggingList.append((lineNumber, processName, timeInformation[0], missionName, pathForDB, eForLater))
                            createLogThread = False
                            

                        else:
                            lock.release()

                    elif "_create_.rsp" in line:

                        processName = "Create"
                        lineNumber = "NA"

                    elif "_ahab_georeferencing_" in line:

                        processName = "Georeferencing"
                        lineNumberIndex = line.split("_").index("georeferencing") + 1
                        lineNumber = line.split("_")[lineNumberIndex] + "_" +line.split("_")[lineNumberIndex+1][:6]

                    #This is the only process that we can have a status bar for.
                    elif "_process_pointcloud_" in line:

                        processName = "Process Pointcloud"
                        lineNumberIndex = line.split("_").index("pointcloud") + 1
                        lineNumber = line.split("_")[lineNumberIndex] + "_" +line.split("_")[lineNumberIndex+1][:6]

                        for inputInformation in inputListOfTuples:
                            if lineNumber == inputInformation[0]:
                                numberOfInputFiles = inputInformation[1]
                                lock.acquire()
                                t = tqdm(total = numberOfInputFiles, unit = 'Files', desc = lineNumber[:3], position = 1, leave = False)
                                lock.release()
                    elif "_ahab_filter_pointcloud_" in line:

                        processName ="Filter Pointcloud"
                        lineNumberIndex = line.split("_").index("pointcloud") + 1
                        lineNumber = line.split("_")[lineNumberIndex] + "_" +line.split("_")[lineNumberIndex+1][:6]
                        #print("Flight Line " + lineNumber + " is being worked on.")
                        for pointCloudInfo in loggingList:
                            if (lineNumber == pointCloudInfo[0]) and (pointCloudInfo[1] == "Process Pointcloud"):
                                pointCloudTime = pointCloudInfo[3].split(":")
                                PCHours = int(pointCloudTime[0])*3600
                                PCMinutes = int(pointCloudTime[1])*60
                                PCSeconds = int(pointCloudTime[2])
                                PCTotal = PCHours + PCMinutes + PCSeconds
                                datetime.time(int(PCHours),int(PCMinutes),int(PCSeconds))
                                currentTimeTuple = time.localtime(time.time())[3:6]
                                currentTime = currentTimeTuple[0] *3600 + currentTimeTuple[1] *60 + currentTimeTuple[2] + (PCTotal/2)
                                print("Flight Line " + lineNumber + " is estimated Completion time is " + str(datetime.timedelta(seconds = currentTime)))

                                
                    elif "_ahab_updateLasReference_" in line:

                        processName ="Update Las Reference"
                        lineNumber = "NA"

                    #This is the final process
                    elif "_updateMeanTerrainHeight_" in line:

                        processName = "Update Mean Terrain Height"
                        lineNumber = "NA"
                        

                    else:

                        if processName == "Process Pointcloud":

                            if "root - Start ingest ..." in line:

                                numberOfLas = 0
                                lock.acquire()
                                t.close()
                                t = tqdm(total = numberOfInputFiles, unit = 'Files', desc = lineNumber[:3], position = 1, leave = False)
                                lock.release()

                            if "Export points to LAS file" in line:

                                lock.acquire()
                                numberOfLas += 1
                                t.update()
                                percent = round(100*(numberOfLas/numberOfInputFiles),2)
                                print(lineNumber + " is " + str(percent) + "% complete with Process Point Cloud at " + str(time.strftime("%H:%M:%S", time.localtime(time.time())))) 
                                t.write("\n" + lineNumber + " is " + str(percent) + "% complete with Process Point Cloud at " + str(time.strftime("%H:%M:%S", time.localtime(time.time()))))
                                lock.release()

                            
    log.close()
    print("Finished " + logName)

     

#This Function Controls the Parent Threads (Might Break)
def Log_Job_Parent(loggingList, pathToLogs, listOfLineNames, inputTupleList, pathName, lock):

    createLogThread = True
    logNameList = list()
    currentLogIndex = 0
    threadList = list()
    pathForDB = pathToLogs[:-5]

    while createLogThread:
        
        lastLogIndex = len(os.listdir(pathToLogs))

        for i in range(currentLogIndex, lastLogIndex):

            logName = os.listdir(pathToLogs)[i]

            if logName not in logNameList:
                sleep(.01)

                logFile = os.path.join(pathToLogs, logName)
                logNameList.append(logName)
                threadName = "Thread-" + logName
                logThread = threading.Thread(target = Log_Parsing_Thread, name = threadName, args = (logFile, inputTupleList, loggingList, createLogThread, pathName, pathForDB, lock))
                logThread.start()
                threadList.append(logThread)
        
        if i + 1 < lastLogIndex:
            currentLogIndex = i + 1

        elif (threading.active_count() == 2) and (len(threadList) > 3):
            createLogThread = False


#This Function Controls the Parent Threads (Might Break)
def Job_Thread_Manager(logPathList, loggingList, listOfLineNames, inputTupleList, lock):
    lock = threading.Lock()
    Check = True
    jobThreadList = list()
    interPathList = list()

    for i in logPathList:

        pathName = i.split("\\")[-2]
        pathToInter = os.path.join(i[:-5],"intermediate")
        interPathList.append(pathToInter)
        jobThread = threading.Thread(target = Log_Job_Parent, name = pathName, args = (loggingList, i, listOfLineNames, inputTupleList, pathName, lock))
        jobThread.start()
        jobThreadList.append(jobThread)
    
    while Check:
        sleep(10)
        if (threading.active_count() == 3) and (len(jobThreadList) >= 1):
            Check = False

##    while Check: 
##        for i in interPathList:
##            if (len(interPathList) >= 2):
##                for j in interPathList:
##                    if (i != j) and (os.path.isdir(i) == False) and (os.path.isdir(j) == False):
##                        Check = False
##            else:
##                if (os.path.isdir(i) == False):
##                    Check = False
##            ##print(threading.enumerate())
    return loggingList

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
"""


"""
#########################################################################################################################################################################################################################################
###                                                                                                   Format Functions                                                                                                                ###
#########################################################################################################################################################################################################################################


def Format_Time(timeString):
    timeList = timeString.split(":")
    timeInSeconds = int(timeList[0])*3600 + int(timeList[1])*60 +int(timeList[2])
    return timeInSeconds

#ISSUE
def Format_Information_For_DB(inputInfo, trackingInfo, outputInfo):

    condensedTrackingInfo = list()
    lineNameList = list()
    condensedJobInfo = list()

    for i in trackingInfo:

        processingTime = 0

        for j in trackingInfo:

            if (i[0] == j[0]) and (i[0] != 'NA') and (i[0] not in lineNameList):
                processingTime += Format_Time(j[3])               

            elif (i[0] == j[0]) and (i[1] == 'Create') and (j[1] == 'Update Mean Terrain Height') and (i[3] == j[3]):
                totalTime = str(j[5] - i[5]) 
                condensedJobInfo.append((i[3], totalTime, j[4]))

        if (i[0] != 'NA') and (i[0] not in lineNameList):
            lineNameList.append(i[0])
            condensedTrackingInfo.append((i[0], processingTime, i[2]))

    lineDatabaseInfo = list()

    for i in inputInfo:

        for j in outputInfo:

            for k in condensedTrackingInfo:

                if(i[0] == j[0] == k[0]):

                    fullLineInfo = (k[2],i[0],i[1],j[1],i[3],j[2],k[1],str(datetime.timedelta(seconds=k[1])),i[2],j[3])
                    lineDatabaseInfo.append(fullLineInfo)
    jobDatabaseInfo = list()
    

    for i in condensedJobInfo:

        totalNumberOfLines = 0
        totalInputSize = 0
        totalOutputSize = 0
        totalInputFiles = 0
        totalOutputFiles = 0

        for j in lineDatabaseInfo:

            if i[0] == j[0]:

                totalInputFiles += j[2]
                totalOutputFiles += j[3]
                totalNumberOfLines += 1
                totalInputSize += j[4]
                totalOutputSize += j[5]
                ppm = j[8]
        projectName = i[2].split("\\")[2]
        fakeLinearMiles = round((totalOutputFiles - totalNumberOfLines) * 0.621371, 2)
        fullJobInfo = (projectName, i[0], totalNumberOfLines, fakeLinearMiles, i[1], totalInputFiles, totalOutputFiles, totalInputSize, totalOutputSize, ppm, i[2])
        jobDatabaseInfo.append(fullJobInfo)
    
    return lineDatabaseInfo, jobDatabaseInfo
    
#########################################################################################################################################################################################################################################
"""


"""
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

def FORMAT_FOR_LINE(con,data):    
    insert_into_line = """INSERT INTO Line
                          (Job_Name, Line_Name, Number_Of_Input_Files, Number_Of_Output_Files, Input_Size_GB, Output_Size_GB, Total_Processing_Time_Seconds, Total_Processing_Time, Point_Density, Region) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    cursor = con.cursor()
    try:
        cursor.execute(insert_into_line, data)
        con.commit()
    except Error as e:
        print(f"The error '{e}' has occurred")
        

def FORMAT_FOR_JOB(con, data):
    insert_into_job = """INSERT INTO Job
                         (Project_Name, Job_Name, Number_of_Flight_Lines, Linear_Miles, Total_Processing_Time, Number_of_Inputs, Number_of_Outputs, Size_GB_In, Size_GB_Out, Point_Density, Path)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    cursor = con.cursor()
    try:
        cursor.execute(insert_into_job, data)
        con.commit()
    except Error as e:
        print(f"The error '{e}' has occurred")

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

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
#########################################################################################################################################################################################################################################
"""


"""
#########################################################################################################################################################################################################################################
###                                                                                                      Main Function                                                                                                                ###
#########################################################################################################################################################################################################################################

def main():
    path = input("Hi please put the file location here:")
    os.chdir(path)
    path = os.getcwd()
    loggingList = list()

    #try:
    #Get Input as inputInformation list
    inputInformation = Get_Input(path) # THIS RETURNS lineInputInformation: [(Name, inlength, PPM, INSIZEGB),...], orderedLineNames [Name,...]
    pathsToOutput = Get_Path_To_Output(path)
    lock = threading.Lock()

    #Track Current Running Process Store as trackingInformation

    trackingInformation = Job_Thread_Manager(pathsToOutput[0], loggingList, inputInformation[1], inputInformation[0], lock) #THIS RETURNS  trackingInformation: [(Name, ProcessName, JobName, ProcessTotalTime),...]
    
    #Get Output as outputInformation list
    outputInformation = Get_Output_Info(pathsToOutput[1]) #THIS RETURNS outputInformation:[(Name, outlength, OUTSIZEGB, OutputMissionName), ...]

    #Format for Database
    formatedData = Format_Information_For_DB(inputInformation[0], trackingInformation, outputInformation)

    #Write to Database
    conn = Create_Connection("\\\\Lidar01\\Vol1\\HxMap_Shared\\Database\\LegacyDataFinal.db")

    Execute_Query(conn, create_job_table)
    Execute_Query(conn, create_line_table)

    for i in formatedData[0]:
        FORMAT_FOR_LINE(conn,i)

    for j in formatedData[1]:
        FORMAT_FOR_JOB(conn,j)

    print(execute_read_query(conn, "SELECT * FROM Line"), end = "\n")
    print(execute_read_query(conn, "SELECT * FROM Job"), end = "\n")
    conn.close()
    #except:
        #print("An error has occured")

if __name__ == "__main__":
    main()

#####################################################################################################         END         ###############################################################################################################
