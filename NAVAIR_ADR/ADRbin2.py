import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import defaultdict
import pickle

store2 = pd.HDFStore('bin/binprestage.h5')
df = store2['all']
dfvars = list(df)
df2 = df.dropna(axis=1, how='all')
df2vars = list(df2)

#print(list(df))
#stats = set([x.split("_")[1] for x in list(set(dfvars)-set(df2vars))])
#print(stats)
#print(list(set(dfvars)-set(df2vars)))
pbar = tqdm(total = len(list(df)))

all_bins = defaultdict(list)
ser = defaultdict(list)
for var in list(df):
    bins = pd.cut(df[var], 5, retbins=True)[1]
    bin_labels = ["{}_{}".format(bins[i], bins[i+1]) for i in range(5)]
    #bins[0],bins[-1] = -np.inf, np.inf
    #bin_labels[0], bin_labels[-1] = "-inf_{}".format(bins[1]), "{}_inf".format(bins[-2])
    all_bins[var] = [bins, bin_labels]
    pbar.update(1)


all_bins

pickle_out = open("bin/bins.pickle","wb")
pickle.dump(all_bins, pickle_out)
pickle_out.close()