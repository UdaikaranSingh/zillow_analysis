import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):

	file_dir =  fips_code + "/"
	gen_path = gen_dir

	columns = ['TransId',
	'DataClassStndCode',
	'RecordTypeStndCode',
	'DocumentTypeStndCode',
	'SalesPriceAmount',
	'IntraFamilyTransferFlag',
	'PropertyUseStndCode',
	'LoanTypeStndCode',
	'primary_date',
	'primary_date_marker',
	'AssessorParcelNumber',
	'PropertySequenceNumber',
	'ImportParcelID']

	reduced_table_path = os.path.join(gen_dir, 'reduced_table.csv')

	df = pd.read_csv(reduced_table_path)[columns]

	df = df.rename(columns = {'primary_date' : 'primary_date_flag',
                          'primary_date_marker' : 'primary_date',
                          'AssessorParcelNumber' : 'AssessorParcelNumber_count',
                          'PropertySequenceNumber' : 'PropertySequenceNumber_count',
                          'ImportParcelID' : 'ImportParcelID_count'})

	df.to_csv(reduced_table_path)

