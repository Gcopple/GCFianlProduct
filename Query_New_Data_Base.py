import sqlite3
from sqlite3 import Error
from time import sleep
import csv
import os
import sys

def create_connection(path):
    connection = None

    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")

    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        connection.commit()
        return cursor
        print("Query executed successfully")

    except Error as e:
        print(f"The error '{e}' occurred")

def write_to_csv(path, data, fileName):

    conn = create_connection(path)
    cursor = execute_query(conn, data)

    try:
        col_name_list = [tuple[0] for tuple in cursor.description]

    except:
        print("No Matches")

    try:
        informationToWrite = cursor.fetchall()
        fileAlreadyThere = os.path.exists(fileName)

    except Error as e:
        print(f"The error '{e}' occurred") 

    if fileAlreadyThere:

        while fileAlreadyThere:
            print("File" + fileName + "Exsits")
            fileChoice = input("Would you like to:(O) Overwrite this file, (A) Append to it, or (C) Create a new file?\n")

            if "o" in fileChoice.lower():
                with open(fileName,"w") as csv_file:
                    csvWriter = csv.writer(csv_file)
                    csvWriter.writerow(col_name_list)
                    csvWriter.writerows(informationToWrite)
                fileAlreadyThere = False

            elif "a" in fileChoice.lower():
                with open(fileName,"a") as csv_file:
                    csvWriter = csv.writer(csv_file)
                    csvWriter.writerow(col_name_list)
                    csvWriter.writerows(informationToWrite)
                fileAlreadyThere = False

            elif "c" in fileChoice.lower():
                fileName = input("Please enter a new file name:")
                fileName += ".csv"

                with open(fileName,"w") as csv_file:
                    csvWriter = csv.writer(csv_file)
                    csvWriter.writerow(col_name_list)
                    csvWriter.writerows(informationToWrite)
                fileAlreadyThere = False

            else:
                print("Please enter either O, A, or C")

    else:
        with open(fileName,"w") as csv_file:
            csvWriter = csv.writer(csv_file)
            csvWriter.writerow(col_name_list)
            csvWriter.writerows(informationToWrite)
    print("Complete")
    conn.close()
    






exit_condition = False
csvName = input("Please input a file name:")
if ".csv" not in csvName:
    csvName += ".csv"
while not exit_condition:
    
    selectList = list()
    userQuery = ''
    firstChoice = input("Hello, would you Like Job information or Line information:")

    if "quit" in firstChoice.lower():
        sys.exit("Thank you")

    #Job Query
    elif "job" in firstChoice.lower():
        secondChoice = input("What information would you like? \n\
Options are: Project Name, Job Name, Number of Flight Lines, \
Linear Miles, Total Processing Time, Number of Inputs, Number of Outputs, Size In, Size Out, Point Density, Path, \
or All\nPlease input options seperated by commas:")
        isChoice = False
        while not isChoice:
            if "quit" in secondChoice.lower():
                sys.exit("Thank you")
            secondChoiceList = secondChoice.split(",")
            for i in secondChoiceList:
                if (len(secondChoiceList) > 1) and (i != secondChoiceList[-1]):
                    if "Project Name".lower() in i.lower():
                        selectList.append("Project_Name, ")
                    elif "Job Name".lower() in i.lower():
                        selectList.append("Job_Name, ")
                    elif "Number of Flight Lines".lower() in i.lower():
                        selectList.append("Number_of_Flight_Lines, ")
                    elif "Linear Miles".lower() in i.lower():
                        selectList.append("Linear_Miles, ")
                    elif "Total Processing Time".lower() in i.lower():
                        selectList.append("Total_Processing_Time, ")
                    elif "Number of Inputs".lower() in i.lower():
                        selectList.append("Number_of_Inputs, ")
                    elif "Number of Outputs".lower() in i.lower():
                        selectList.append("Number_of_Outputs, ")
                    elif "Size In".lower() in i.lower():
                        selectList.append("Size_GB_In, ")
                    elif "Size Out".lower() in i.lower():
                        selectList.append("Size_GB_Out, ")
                    elif "Point Density".lower() in i.lower():
                        selectList.append("Point_Density, ")
                    elif "Path".lower() in i.lower():
                        selectList.append("Path, ")   
                elif "all" in i.lower():
                    selectList.append("* ")
                    isChoice = True
                elif "Project Name".lower() in i.lower():
                    selectList.append("Project_Name ")
                    isChoice = True
                elif "Job Name".lower() in i.lower():
                    selectList.append("Job_Name ")
                    isChoice = True
                elif "Number of Flight Lines".lower() in i.lower():
                    selectList.append("Number_of_Flight_Lines ")
                    isChoice = True
                elif "Linear Miles".lower() in i.lower():
                    selectList.append("Linear_Miles ")
                    isChoice = True
                elif "Total Processing Time".lower() in i.lower():
                    selectList.append("Total_Processing_Time ")
                    isChoice = True
                elif "Number of Inputs".lower() in i.lower():
                    selectList.append("Number_of_Inputs ")
                    isChoice = True
                elif "Number of Outputs".lower() in i.lower():
                    selectList.append("Number_of_Outputs ")
                    isChoice = True
                elif "Size In".lower() in i.lower():
                    selectList.append("Size_GB_In ")
                    isChoice = True
                elif "Size Out".lower() in i.lower():
                    selectList.append("Size_GB_Out ")
                    isChoice = True
                elif "Point Density".lower() in i.lower():
                    selectList.append("Point_Density ")
                    isChoice = True
                elif "Path".lower() in i.lower():
                    selectList.append("Path ")
                    isChoice = True
                else:
                    print("ERROR: Please use at least one of the options (Project Name, Job Name, Number of Flight Lines, \
Linear Miles, Total Processing Time, Size, Point Density, Path,\
or All)")
                    sleep(1)
                    secondChoice = input("Please input options seperated by commas:")

            if len(selectList) > 0:
                userQuery = "SELECT "

                for i in selectList:
                    userQuery += i
                userQuery += "FROM Job "

                
                isChoice = False
                while not isChoice:
                    thirdChoice = input("Do you want a specific Job, Project, etc..?\nYes or No:")
                    if "n"in thirdChoice.lower():
                        isChoice = True
                        FinalChoice = False
                        print("So from the Job Table we will get %s" %secondChoice)
                        while not FinalChoice:
                            sixthChoice = input("Is this correct?\nYes or No: ")
                            if "quit" in sixthChoice.lower():
                                sys.exit("Thank you")
                                FinalChoice = True

                            elif "y" in sixthChoice.lower():
                                write_to_csv("\\\\Lidar01\\Vol1\\HxMap_Shared\\Database\\LegacyDataFinal.db",userQuery,csvName)
                                FinalChoice = True

                            elif "n" in sixthChoice.lower():
                                "Starting Over"
                                FinalChoice = True

                            else:
                                print("Please enter either Yes or No")
                    
                    elif "quit" in thirdChoice.lower():
                        sys.exit("Thank you")

                    elif "y" in thirdChoice.lower():
                        isChoice = True
                        
                        while "WHERE" not in userQuery:
                            fourthChoice = input("Which column is the option in?\nInput:")
                            if "quit" in fourthChoice.lower():
                                sys.exit("Thank you")
                            elif "Project".lower() in fourthChoice.lower():
                                userQuery += "WHERE Project_Name "
                            elif "Job Name".lower() in fourthChoice.lower():
                                userQuery += "WHERE Job_Name "
                            elif "Number of Flight Lines".lower() in fourthChoice.lower():
                                userQuery += "WHERE Number_of_Flight_Lines "
                            elif "Total Processing Time".lower() in fourthChoice.lower():
                                userQuery += "WHERE Total_Processing_Time "
                            elif "Linear Miles".lower() in fourthChoice.lower():
                                userQuery += "WHERE Linear_Miles "
                            elif "Number of Inputs".lower() in fourthChoice.lower():
                                userQuery += "WHERE Number_of_Inputs "
                            elif "Number of Outputs".lower() in fourthChoice.lower():
                                userQuery += "WHERE Number_of_Outputs "
                            elif "Size In".lower() in fourthChoice.lower():
                                userQuery += "WHERE Size_GB_In "
                            elif "Size Out".lower() in fourthChoice.lower():
                                userQuery += "WHERE Size_GB_out "
                            elif "Point Density".lower() in fourthChoice.lower():
                                userQuery += "WHERE Point_Density "
                            elif "Path".lower() in fourthChoice.lower():
                                userQuery += "WHERE Path "
                            else:
                                if "all".lower() in secondChoice:
                                    print("Not a valid option. Please Choose an option from: Project Name, Job Name, Number of Flight Lines, \
Linear Miles, Total Processing Time, Size, Point Density, Path")
                                else:
                                    print("Not a valid option. Please Choose an option from: " + secondChoice)
                        
                        
                        fithChoice = input("What is the constraint on this column?\nInput:")

                        if "quit" in fithChoice.lower():
                                sys.exit("Thank you")

                        userQuery += "LIKE '%" + fithChoice + "%'"
                        print("Query " + userQuery + " will be exicuted")

                        sixthCheck = True
                        while sixthCheck:
                            sixthChoice = input("Is this correct?\nYes or No: ")

                            if "quit" in sixthChoice.lower():
                                sys.exit("Thank you")
                                
                        
                            elif "y" in sixthChoice.lower():
                                write_to_csv("\\\\Lidar01\\Vol1\\HxMap_Shared\\Database\\LegacyDataFinal.db",userQuery,csvName)
                                sixthCheck = False

                            elif "n" in sixthChoice.lower():
                                sixthCheck = False

                            else:
                                print("Please enter either Yes or No")
                    else:
                                print("Please enter either Yes or No")

    #Line Query
    elif  "line" in firstChoice.lower():
        secondChoice = input("What information would you like? \n\
Options are: Job Name, Line Name, Input Files, Output Files, Input Size, Output Size, Total Time in Seconds, Total Processing Time, Density, Region, or All \nPlease input options seperated by commas:")

        isChoice = False
        while not isChoice:

            if "quit" in secondChoice.lower():
                sys.exit("Thank you")
            secondChoiceList = secondChoice.split(",")

            for i in secondChoiceList:
                if (len(secondChoiceList) > 1) and (i != secondChoiceList[-1]):
                    if "Job".lower() in i.lower():
                        selectList.append("Job_Name, ")
                    elif "Line Name".lower() in i.lower():
                        selectList.append("Line_Name, ")
                    elif "Input Files".lower() in i.lower():
                        selectList.append("Number_Of_Input_Files, ")
                    elif "Output Files".lower() in i.lower():
                        selectList.append("Number_Of_Output_Files, ")
                    elif "Output Size".lower() in i.lower():
                        selectList.append("Output_Size_GB, ")
                    elif "Input Size".lower() in i.lower():
                        selectList.append("Input_Size_GB, ")
                    elif "Total Time in Seconds".lower() in i.lower():
                        selectList.append("Total_Processing_Time_Seconds, ")
                    elif "Total Processing Time".lower() in i.lower():
                        selectList.append("Total_Processing_Time, ")
                    elif "Density".lower() in i.lower():
                        selectList.append("Point_Density, ")
                    elif "Region".lower() in i.lower():
                        selectList.append("Region, ")
                elif "all" in  i.lower():
                    selectList.append("* ")
                elif "Job".lower() in i.lower():
                    selectList.append("Job_Name ")
                elif "Line Name".lower() in i.lower():
                    selectList.append("Line_Name ")
                elif "Input Files".lower() in i.lower():
                    selectList.append("Number_Of_Input_Files ")
                elif "Output Files".lower() in i.lower():
                    selectList.append("Number_Of_Output_Files ")
                elif "Input Size".lower() in i.lower():
                    selectList.append("Input_Size_GB ")
                elif "Output Size".lower() in i.lower():
                    selectList.append("Output_Size_GB ")
                elif "Total Time in Seconds".lower() in i.lower():
                    selectList.append("Total_Processing_Time_Seconds ")
                elif "Total Processing Time".lower() in i.lower():
                    selectList.append("Total_Processing_Time ")
                elif "Density".lower() in i.lower():
                    selectList.append("Point_Density ")
                elif "Region".lower() in i.lower():
                    selectList.append("Region ")
                else:
                    print("ERROR: Please use at least one of the options (Options are: Job Name, Line Name, Input Files, Output Files, Output Size, Total Time in Seconds, Total Processing Time, Georeferencing Total Time, Process Pointcloud Total Time, Filter Point Cloud Total Time)")
                    sleep(1)
                    secondChoice = input("Please input options seperated by commas:")

            if len(selectList) > 0:
                userQuery = "SELECT "

                for i in selectList:
                    userQuery += i
                
                userQuery += "FROM Line "

                isChoice = False
                while not isChoice:
                    thirdChoice = input("Do you want a specific Job, Project, etc..?\nYes or No:")
                    if "n"in thirdChoice.lower():
                        isChoice = True
                        FinalChoice = False
                        print("So from the Line Table we will get %s" %secondChoice)
                        while not FinalChoice:
                            sixthChoice = input("Is this correct?\nYes or No: ")
                            if "quit" in sixthChoice.lower():
                                sys.exit("Thank you")
                                FinalChoice = True

                            elif "y" in sixthChoice.lower():
                                write_to_csv("\\\\Lidar01\\Vol1\\HxMap_Shared\\Database\\LegacyDataFinal.db",userQuery,csvName)
                                FinalChoice = True

                            elif "n" in sixthChoice.lower():
                                "Starting Over"
                                FinalChoice = True

                            else:
                                print("Please enter either Yes or No")
                    
                    elif "quit" in thirdChoice.lower():
                        sys.exit("Thank you")

                    elif "y" in thirdChoice.lower():
                        isChoice = True

                        while "WHERE" not in userQuery:
                            fourthChoice = input("Which column is the option in?\nInput:")
                            if "quit" in fourthChoice.lower():
                                sys.exit("Thank you")
                            elif ("Job Name".lower() in fourthChoice.lower()) and ("Line Name".lower() in fourthChoice.lower()):
                                userQuery += "WHERE Line_ID "
                            elif "Job Name".lower() in fourthChoice.lower():
                                userQuery += "WHERE Job_Name "
                            elif "Input Files".lower() in fourthChoice.lower():
                                userQuery += "WHERE Number_Of_Intput_Files "
                            elif "Output Files".lower() in fourthChoice.lower():
                                userQuery += "WHERE Number_Of_Output_Files "
                            elif "Input Size".lower() in fourthChoice.lower():
                                userQuery += "WHERE Input_Size_GB "
                            elif "Output Size".lower() in fourthChoice.lower():
                                userQuery += "WHERE Output_Size_GB "
                            elif "Total Time in Seconds".lower() in fourthChoice.lower():
                                userQuery += "WHERE Total_Processing_Time_Seconds "
                            elif "Total Processing Time".lower() in fourthChoice.lower():
                                userQuery += "WHERE Total_Processing_Time "
                            elif "Density".lower() in fourthChoice.lower():
                                userQuery += "WHERE Point_Density "
                            elif "Region".lower() in fourthChoice.lower():
                                userQuery += "WHERE Region "
                            elif "Line Name".lower() in fourthChoice.lower():
                                userQuery += "WHERE Line_Name "
                            else:
                                if "all".lower() in secondChoice:
                                    print("Not a valid option. Please Choose an option from: Job Name, Line Name, Input Files, Output Files, Input Size, Output Size, Total Time in Seconds, Total Processing Time, Density, Region, Line Name")
                                else:
                                    print("Not a valid option. Please Choose an option from: " + secondChoice)

                        fithChoice = input("What is the constraint on this column?\nInput:")

                        if "quit" in fithChoice.lower():
                            sys.exit("Thank you")
                    
                        userQuery += "LIKE '%" + fithChoice + "%'"
                        print("Query " + userQuery + " will be exicuted")\


                        sixthCheck = True
                        while sixthCheck:
                            sixthChoice = input("Is this correct?\nYes or No: ")

                            if "quit" in sixthChoice.lower():
                                sys.exit("Thank you")
                                
                        
                            elif "y" in sixthChoice.lower():
                                write_to_csv("\\\\Lidar01\\Vol1\\HxMap_Shared\\Database\\LegacyDataFinal.db",userQuery,csvName)
                                sixthCheck = False

                            elif "n" in sixthChoice.lower():
                                sixthCheck = False

                            else:
                                print("Please enter either Yes or No")
                    else:
                                print("Please enter either Yes or No")
    
##    elif "both" in firstChoice.lower():
##        
##        secondChoice = input("What would you like from the Job Table:")
##        thirdChoice = input("What would you like from the Line Table:")

        
 
