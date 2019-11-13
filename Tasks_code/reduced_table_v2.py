import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):

	file_dir =  fips_code + "/"
	gen_path = gen_dir
	main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
    usecols =['TransId','DocumentTypeStndCode','DataClassStndCode',
              'IntraFamilyTransferFlag','LoanTypeStndCode','PropertyUseStndCode',
             'RecordTypeStndCode','SalesPriceAmount','SalesPriceAmountStndCode'])

	date_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["TransId", "DocumentDate", "SignatureDate", "RecordingDate"])

	def get_primary_date(row):
	    if not pd.isna(row['DocumentDate']):
	        return 0
	    elif not pd.isna(row['SignatureDate']):
	        return 1
	    else:
	        return 2

	def get_primary_date_marker(row):
	    if not pd.isna(row['DocumentDate']):
	        return row['DocumentDate']
	    elif not pd.isna(row['SignatureDate']):
	        return row['SignatureDate']
	    else:
	        return np.nan

	primary_dates_marker = date_table.apply(get_primary_date_marker, axis = 1)
	priamry_dates = date_table.apply(get_primary_date, axis = 1)
	main_table['primary_date'] = priamry_dates
	main_table['primary_date_marker'] = primary_dates_marker

	prop_table = SingleTableReader("ZTrans", "PropertyInfo", file_dir).read(
		usecols = ['TransId','AssessorParcelNumber',
		'ImportParcelID',
		'PropertySequenceNumber'])

	squeezed_prop_table = prop_table.groupby("TransId").count()
	df = main_table.merge(squeezed_prop_table, how = "left", on='TransId')
	path = os.path.join(gen_dir, 'reduced_table.csv')

	df.to_csv(path)


	