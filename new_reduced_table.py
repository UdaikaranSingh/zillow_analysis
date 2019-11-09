import numpy as np
import pandas as pd
import os

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):
	file_dir =  fips_code + "/"
	gen_path = gen_dir

	primary_dates_file = "primary_dates.csv"
	reduced_table_file = 'reduced_table.csv'

	cols = ["TransId", "IntraFamilyTransferFlag", "DocumentTypeStndCode", 
	"LoanTypeStndCode", "PropertyUseStndCode",
	"RecordTypeStndCode", "SalesPriceAmount",
	"SalesPriceAmountStndCode"]

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =cols)

	dates_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =['TransId','DocumentDate','SignatureDate','RecordingDate'])

	def get_time_marker(row):
		if not pd.isna(row['DocumentDate']):
			return 0
		if not pd.isna(row['SignatureDate']):
			return 1
		else:
			return 2

	dates_table['date_marker'] = dates_table.apply(get_time_marker, axis = 1)
	dates_table = dates_table[['TransId', 'date_marker']]

	apn_table = SingleTableReader("ZTrans", "PropertyInfo", file_dir).read(usecols =['TransId','AssessorParcelNumber'])

	primary_date_table = pd.read_csv(os.path.join(gen_path,primary_dates_file),usecols=['TransId', 'PrimaryDate'])

	transaction_main_table = transaction_main_table.merge(primary_date_table, on='TransId', how = 'left')
	transaction_main_table = transaction_main_table.merge(apn_table, on='TransId', how = 'left')
	transaction_main_table = transaction_main_table.merge(dates_table, on='TransId', how = 'left')
	old_reduced_table = pd.read_csv(os.path.join(gen_path,reduced_table_file)).drop(columns = ['Unnamed: 0'])
	transaction_main_table = transaction_main_table.merge(old_reduced_table, on='TransId', how = 'left')
	transaction_main_table = transaction_main_table.astype({"ImportParcelID_count": "int8",
                                                        "date_marker": "int8",
                                                        "FIPS": "int8",
                                                        "PropertySequenceNumber_count": "int8"})

	transaction_main_table.to_csv(os.path.join(gen_path, reduced_table_file))




