import numpy as np
import pandas as pd
import os

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):
	file_dir =  fips_code + "/"

	#reading in the data
	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["TransId", "DocumentDate", "SignatureDate", "RecordingDate"])

	#actual processing

	#create generated directory to save results
	gen_dir_for_state = os.path.join(gen_dir, file_dir)
	if not os.path.exists(gen_dir):
		os.mkdir(gen_dir)

	if not os.path.exists(gen_dir_for_state):
		os.mkdir(gen_dir_for_state)		

	#program

	def get_primary_date(row):
	    if not pd.isna(row['DocumentDate']):
	        return row['DocumentDate']
	    elif not pd.isna(row['SignatureDate']):
	        return row['SignatureDate']
	    else:
	        return row['RecordingDate']

    primary_dates = transaction_main_table.apply(get_primary_date, axis = 1)

    df = pd.DataFrame(columns = ["TransId", "PrimaryDate"])
    
    df['TransId'] = transaction_main_table['TransId']
    df['PrimaryDate'] = primary_dates

    primary_dates_path = os.path.join(gen_dir_for_state, "primary_dates.csv")
    df.to_csv(primary_dates_path)