import numpy as np
import pandas as pd
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

set_of_values = {"S","D", "H", "F", "M",  "P"}

def program(fips_code, gen_dir = os.getcwd()):
	
	file_dir =  fips_code + "/"

	if not os.path.exists(os.path.join(gen_dir, "conditional_dist")):
		os.mkdir(os.path.join(gen_dir, "conditional_dist"))
	
	gen_dir = os.path.join(gen_dir, "conditional_dist")

	#reading in the data
	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["DocumentTypeStndCode", "DataClassStndCode"])

	s1 = set(transaction_main_table.DataClassStndCode.unique())
	s2 = s1 - (set_of_values - s1)

	DataClassCodes = list(s2)

	for code in DataClassCodes:
	    df_temp = transaction_main_table[transaction_main_table["DataClassStndCode"] == code]
	    df_temp = pd.value_counts(df_temp.DocumentTypeStndCode, normalize=True)[0:5]
	    df_temp.plot.bar()
	    title = "Distribution of Document Types Relative to Data Class: {}".format(code)
	    plt.title(title)
	    name = os.path.join(gen_dir,"document_type_relative_to_data_class_{}".format(code))
	    plt.savefig(name, dpi = 320)
	    plt.figure()

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
    	usecols =["DocumentTypeStndCode", "IntraFamilyTransferFlag"])

	temp_df = transaction_main_table[transaction_main_table.IntraFamilyTransferFlag == "Y"]
	pd.value_counts(temp_df.DocumentTypeStndCode, normalize = True)[0:5].plot.bar()
	name = "document_type_relative_to_IntraFamilyTransferFlag"
	path_img = os.path.join(gen_dir, name)
	plt.savefig(path_img, dpi = 320)
	plt.figure()

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["DocumentTypeStndCode", "LoanTypeStndCode"])

	loan_type_codes = set(transaction_main_table.LoanTypeStndCode.unique())

	for loan_type in loan_type_codes:
		df_temp = transaction_main_table[transaction_main_table["LoanTypeStndCode"] == loan_type]
		if (len(df_temp) < 100):
			continue
		df_temp = pd.value_counts(df_temp.DocumentTypeStndCode, normalize=True)[0:5]
		df_temp.plot.bar()
		title = "Distribution of Document Types Relative to Loan Type: {}".format(loan_type)
		plt.title(title)
		name = os.path.join(gen_dir,"document_type_relative_to_loan_type_{}".format(loan_type))
		plt.savefig(name, dpi = 320)
		plt.figure()

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["DocumentTypeStndCode", "RecordTypeStndCode"])

	record_type_values = list(transaction_main_table.RecordTypeStndCode.unique())
	for rec_type in record_type_values:
		df_temp = transaction_main_table[transaction_main_table["RecordTypeStndCode"] == rec_type]
		if (len(df_temp) < 100):
			continue
		df_temp = pd.value_counts(df_temp.DocumentTypeStndCode, normalize=True)[0:5]
		df_temp.plot.bar()
		title = "Distribution of Document Types Relative to Record Type: {}".format(rec_type)
		plt.title(title)
		name = os.path.join(gen_dir,"document_type_relative_to_record_type_{}".format(rec_type))
		plt.savefig(name, dpi = 320)
		plt.figure()

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["DocumentTypeStndCode", "SalesPriceAmount"])

	pd.value_counts(transaction_main_table[pd.isna(transaction_main_table.SalesPriceAmount)].DocumentTypeStndCode,
               normalize = True)[0:5].plot.bar()
	title = "Distribution of Document Types Relative SalesPrice Being Nan"
	plt.title(title)
	name = os.path.join(gen_dir,"document_type_relative_to_nan_sales_price")
	plt.savefig(name, dpi = 320)

	