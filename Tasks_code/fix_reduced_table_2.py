import numpy as np
import pandas as pd
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):
	
	file_dir =  fips_code + "/"

	transaction_main_table = SingleTableReader("ZTrans", "Main", file_dir).read(
		usecols =["TransId", "SalesPriceAmountStndCode", "FIPS"])

	reduced_table = 'reduced_table.csv'
	df = pd.read_csv(os.path.join(gen_dir, reduced_table))

	df['SalesPriceAmountStndCode'] = transaction_main_table['SalesPriceAmountStndCode']
	df['FIPS'] = transaction_main_table['FIPS']

	df = df.set_index("TransId")
	df = df.drop(columns=df.columns[0])
	df.to_csv(os.path.join(gen_dir, reduced_table))
