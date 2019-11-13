import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):

	file_dir =  fips_code + "/"
	gen_path = gen_dir

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["TransId", "DocumentDate", "SignatureDate", "RecordingDate"])

	def get_primary_date(row):
	    if not pd.isna(row['DocumentDate']):
	        return 0
	    elif not pd.isna(row['SignatureDate']):
	        return 1
	    else:
	        return 2

	primary_dates_marker = transaction_main_table.apply(get_primary_date, axis = 1)

	primary_dates = pd.read_csv(os.path.join(gen_dir, "primary_dates.csv"),
		usecols = ['TransId', 'PrimaryDate'])

	main_table = pd.read_csv(os.path.join(gen_dir, "reduced_table.csv"))

	main_table['primary_dates_marker'] = primary_dates_marker

	fin_df = primary_dates.merge(primary_dates, how = "left", on = "TransId")

	fin_df = main_table.merge(fin_df, how = "left", on = "TransId")

	path = os.path.join(gen_dir, "reduced_table.csv")

	fin_df.to_csv(path)
