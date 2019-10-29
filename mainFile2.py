import os
#from pathlib import Path
from subprocess import Popen
import zipfile

from task2_3a import *
gen_directory = "/Users/udaisingh/Desktop/gen"

FIPS = ["02"]

def process(fip):
    print("Current FIP processed: " + fip)
    download_command = "aws s3 cp s3://zillow-data-raw/20191001/" + fip + ".zip ."
    #download zip file
    os.system(download_command)
    print("Zip File Downloaded")
    
    #unzip file
    fileName = fip + ".zip"
    unzip_command = "unzip " + fileName + " -d " + fileName.strip(".zip") 
    os.system(unzip_command)
    print("File Unzipped")
    
    folder = fileName.strip(".zip") + "/"
    copy_command = "cp Layout.xlsx " + str(folder)
    os.system(copy_command)
    
    #remove zip file
    remove_zip_file = "rm -rf " + fileName
    os.system(remove_zip_file)

    print("Program Begining")
    program(fip, gen_directory)
    print("Program Ended")

    remove_folder_command = "rm -rf " + fip + "/"
    os.system(remove_folder_command)    


for fip in FIPS:
    process(fip)