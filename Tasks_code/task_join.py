import numpy as np
import pandas as pd
import os

from ZillowUnivariate.univ_stats import SingleTableReader
from optimize_pandas import optimize

def program(fips_code, gen_dir, state_dict, table, columns, newfileName):

	file_dir =  fips_code + "/"

	#############################################
	# Finding indices of arms length transactions
	#############################################

	read_in_table = SingleTableReader("ZTrans", table, file_dir).read(usecols =["TransId"])
	armsLengthTransactionID = state_dict[fips_code].rename(columns = {'transid': 'TransId'})

	transaction_main_table = SingleTableReader("ZTrans", table, file_dir).read(usecols =["TransId"])
	transaction_main_table['index'] = np.arange(len(transaction_main_table))
	corrrect_indices = list(transaction_main_table.merge(armsLengthTransactionID, on = 'TransId', how = "inner")['index'])
	n = len(transaction_main_table)
	rows_to_skip = list(set(range(n)) - set(corrrect_indices))

	#############################################
	# Writing out new file
	#############################################
	columns = ["TransId"] + columns

	table_to_write = SingleTableReader("ZTrans", table, file_dir).read(
		skiprows = rows_to_skip, usecols = columns)

	table_to_write = optimize(table_to_write)
	
	generated_file_name = newfileName
	generated_file_path = os.path.join(gen_dir,generated_file_name)
	table_to_write.to_csv(generated_file_path)
	