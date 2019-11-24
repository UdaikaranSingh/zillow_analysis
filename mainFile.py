import os
#from pathlib import Path
from subprocess import Popen
import zipfile

from Tasks_code.task_join import *


gen_directory = "/Users/udaisingh/Desktop/gen"
arms_length_file_directory = "/Users/udaisingh/Desktop/Education/Y3Q1/dsc_research/arms_length_transids.dta"
download = True

###################################################
# Reads in and organizes arms_length transactions
###################################################
print("reading in arms length file")
reader = pd.read_stata(arms_length_file_directory, iterator = False)
state_dict = {}
grouped = reader.groupby('state_fips')
for i, sdf in grouped:
    state_dict[i] = sdf
del reader, grouped
print("finished reading in arms length file")



###################################################
# parameters you can change to change what is saved
###################################################
table = "Main"
columns = ["SalesPriceAmountStndCode", "FIPS"]
generated_file_name = "place_name_here"


FIPS = ["02"]

"""
FIPS = ["01","02", "04", "05", "06", "08",
"09", "10", "11", "12", "13", "15", "16", "17",
"18", "19", "20", "21", "22", "23", "24", "25",
"26", "27", "28", "29", "30", "31", "32", "33",
"34", "35", "36", "37", "38", "39", "40", "41",
"42", "44", "45", "46", "47", "48", "49", "50",
"51", "53", "54", "55", "56"]
"""

def process(fip):
    print("Current FIP processed: " + fip)
    if (download):
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

    print("Program Beginning")
    gen_dir = os.path.join(gen_directory, fip)
    program(fip, gen_dir, state_dict, table, columns, generated_file_name)
    print("Program Ended")

    if (download):
        remove_folder_command = "rm -rf " + fip + "/"
        os.system(remove_folder_command)    


for fip in FIPS:
    process(fip)