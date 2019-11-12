import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):

	file_dir =  fips_code + "/"
	gen_path = gen_dir
	
	
	table = SingleTableReader("ZTrans", "Main", file_dir).read(
	usecols =['DocumentTypeStndCode','DataClassStndCode','IntraFamilyTransferFlag'])

	f, (ax1, ax2) = plt.subplots(1,2, sharey=True)
	plt.subplots_adjust(hspace = 1)
	(table
	      .groupby('DocumentTypeStndCode')
	      .apply(lambda x: np.mean(x['DataClassStndCode'] == "F"))
	      .to_frame()
	      .rename(columns = {0:"percentage"})
	      .sort_values("percentage", ascending=False)
	      .iloc[0:5]
	).plot.bar(ax= ax1)
	ax1.title.set_text("Distribution Relative to F")

	(table
	      .groupby('DocumentTypeStndCode')
	      .apply(lambda x: np.mean(x['DataClassStndCode'] == "M"))
	      .to_frame()
	      .rename(columns = {0:"percentage"})
	      .sort_values("percentage", ascending=False)
	      .iloc[0:5]
	).plot.bar(ax= ax2)
	ax2.title.set_text("Distribution Relative to M")

	file_path = os.path.join(gen_path, "distribution_of_M_F_relaitve_to_DocumentType")
	plt.savefig(file_path)
	plt.figure()

	f, (ax1, ax2) = plt.subplots(1,2, sharey=True)
	(
	    table
	    .groupby("DataClassStndCode")
	    .apply(lambda x: np.mean(pd.isna(x.IntraFamilyTransferFlag)))
	    .to_frame()
	    .rename(columns = {0: "Percentage"})[0:5]
	    .sort_values("Percentage", ascending = False)
	).plot.bar(ax = ax1)
	ax1.title.set_text("When Intrafamily Tag is Null")

	(
	    table
	    .groupby("DataClassStndCode")
	    .apply(lambda x: np.mean(x.IntraFamilyTransferFlag == "Y"))
	    .to_frame()
	    .rename(columns = {0: "Percentage"})[0:5]
	    .sort_values("Percentage", ascending = False)
	).plot.bar(ax = ax2)
	ax2.title.set_text("When Intrafamily Tag is Y")

	file_path = os.path.join(gen_path, "distribution_of_IntraFamily_Tag_Relative_to_DocumentType")
	plt.savefig(file_path)
	plt.figure()