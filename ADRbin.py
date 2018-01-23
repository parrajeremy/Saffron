import os
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Lock
import random
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")

def addindex(df,i):
    df.index = [i[1:]]
    pbar.update(1)
    return df

#################################################################################################################
# Prepare Environment variables

# Setup store
store = pd.HDFStore('bin/flightstore.h5')
print("Loading processed flight keys. This could take some time")
store_keys = store.keys()
pbar = tqdm(total=len(store_keys))
df = pd.concat([addindex(store[k],k) for k in store_keys])

store2 = pd.HDFStore('bin/binprestage.h5')
store2['all'] = df

