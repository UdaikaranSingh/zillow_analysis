import numpy as np
import pandas as pd
import os

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):
	file_dir =  fips_code + "/"

	#reading in the data
	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["TransId", "State", "FIPS", "BatchID"])

	transaction_property_info = SingleTableReader("ZTrans", "PropertyInfo", file_dir).read(
		usecols= ["TransId", "AssessorParcelNumber","ImportParcelID", "PropertySequenceNumber"])

	assessment_main_table = SingleTableReader("ZAsmt", "Main", file_dir).read(usecols= ["ImportParcelID", "FIPS"])

	#actual processing

	#create generated directory to save results
	gen_dir_for_state = os.path.join(gen_dir, file_dir)
	if not os.path.exists(gen_dir):
		os.mkdir(gen_dir)

	if not os.path.exists(gen_dir_for_state):
		os.mkdir(gen_dir_for_state)		

	temp = pd.merge(transaction_main_table, transaction_property_info, on = "TransId", how="left")

	percent_null_df = temp.apply(lambda x: pd.isna(x).mean()).to_frame().rename(columns = {0:"percentage null"})

	null_file_path = os.path.join(gen_dir_for_state, "null_analysis.csv")
	percent_null_df.to_csv(null_file_path)

	group_temp = temp.groupby("TransId", as_index=False)["PropertySequenceNumber","ImportParcelID"].count().rename(
    	columns = {"PropertySequenceNumber":"PropertySequenceNumber_count", 
    	"ImportParcelID": "ImportParcelID_count"})

	fin_table = pd.merge(temp, group_temp)[["TransId", "State", "FIPS", "PropertySequenceNumber_count", "ImportParcelID_count"]]

	file_path = os.path.join(gen_dir_for_state, "reduced_table.csv")
	fin_table.to_csv(file_path)