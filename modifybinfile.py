import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import defaultdict
import pickle

pickle_in = open("bin/bins.pickle","rb")
binfilter = pickle.load(pickle_in)
binfilter_modify = binfilter

# Load Variable Description File
variables = pd.read_csv("bin/VarDesc.csv")
variables.set_index(['Name'], inplace=True)

df = variables[(variables['min'].notnull()) & (variables['process'] == "y") & (variables['Type'] == "c")]
vars = df.index.tolist()

for var in vars:
    #add bin cutoff
    min = df.ix[var, 'min']
    max = df.ix[var, 'max']


    binfilter_modify["{}_min".format(var)][0] = [-np.inf, min, np.inf]
    binfilter_modify["{}_max".format(var)][0] = [-np.inf, max, np.inf]

    binfilter_modify["{}_min".format(var)][1]=["out-of-range","in-range"]
    binfilter_modify["{}_max".format(var)][1] =["in-range","out-of-range"]


#print(binfilter_modify)

pickle_out = open("bin/bins_mod.pickle","wb")
pickle.dump(binfilter_modify, pickle_out)
pickle_out.close()