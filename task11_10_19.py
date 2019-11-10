import numpy as np
import pandas as pd
import os

from ZillowUnivariate.univ_stats import SingleTableReader

def program(fips_code, gen_dir = os.getcwd()):
	gen_path = gen_dir
	dates_table = SingleTableReader("ZTrans", "Main", file_dir).read(
    	usecols =['TransId','DocumentDate','SignatureDate','RecordingDate'])

	def document_minus_signature(row):
		doc = row['DocumentDate']
		sig = row['SignatureDate']
		if (pd.isna(doc) or pd.isna(sig)):
			return np.nan
		else:
			return doc - sig

	def recording_minus_signature(row):
		recording = row['RecordingDate']
		sig = row['SignatureDate']
		if (pd.isna(recording) or pd.isna(sig)):
			return np.nan
		else:
			return recording - sig

	doc_minus_sig = dates_table.apply(document_minus_signature, axis = 1).dropna()
	rec_minus_sig = dates_table.apply(recording_minus_signature, axis = 1).dropna()
	file_to_write = os.path.join(gen_path,"time_diff.txt")

	#
	# Note: the document minus signature date comes first
	# Then, the recording minus signature date
	#
	with open(file_to_write, "w") as f:
	    if len(doc_minus_sig) == 0:
	        f.write("100\n")
	        f.write("100\n")
	        f.write("100\n")
	    else:
	        f.write(str(np.percentile(doc_minus_sig, 10) + '\n'))
	        f.write(str(np.mean(doc_minus_sig) + '\n'))
	        f.write(str(np.percentile(doc_minus_sig, 90) + '\n'))
	    if len(rec_minus_sig) == 0:
	        f.write("100\n")
	        f.write("100\n")
	        f.write("100\n")
	    else:
	        f.write(str(np.percentile(rec_minus_sig, 10) + '\n'))
	        f.write(str(np.mean(rec_minus_sig) + '\n'))
	        f.write(str(np.percentile(rec_minus_sig, 90) + '\n'))

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
	ax1.title.set_text("Distribution Relative to M")

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

