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

	prop_table = SingleTableReader("ZTrans", "PropertyInfo", file_dir).read(
		usecols = ['TransId','AssessorParcelNumber',
		'ImportParcelID',
		'PropertySequenceNumber'])

	squeezed_prop_table = prop_table.groupby("TransId").count()
	df = main_table.merge(squeezed_prop_table, how = "left", on='TransId')
	path = os.path.join(gen_dir, 'reduced_table.csv')

	df.to_csv(path)


	