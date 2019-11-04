import numpy as np
import pandas as pd
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from ZillowUnivariate.univ_stats import SingleTableReader

bins_sizes = [5,10,15,20,25,30,35,40]

def program(fips_code, gen_dir = os.getcwd()):

	gen_path = gen_dir

	primary_dates_file = "primary_dates.csv"

	dates_path = os.path.join(gen_path,primary_dates_file)

	df = pd.read_csv(dates_path, usecols=['PrimaryDate'])

	timesSeries = pd.to_datetime(df.PrimaryDate)

	timesSeries = timesSeries[timesSeries.dt.year >= 1985]

	if not os.path.exists(os.path.join(gen_path, "primary_date_dist")):
		os.mkdir(os.path.join(gen_path, "primary_date_dist"))

	for bin_val in bins_sizes:
		plt.hist(timesSeries, bins = bin_val)
		plt.title("Histogram With {} bins".format(bin_val))
		name = "primary_dates_hist_{}".format(bin_val)
		name = os.path.join(os.path.join(gen_path, "primary_date_dist"), name)
		plt.savefig(name)
		plt.figure()
