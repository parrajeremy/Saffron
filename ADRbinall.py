import pandas as pd
import os
from tqdm import tqdm
from collections import defaultdict
import pickle
import warnings

warnings.filterwarnings("ignore")



def addindex(df,i):
    df.index = [i[1:]]
    pbar.update(1)
    return df

#################################################################################################################
# Prepare Environment variables
pickle_in = open("bin/bins_mod.pickle","rb")
binfilter = pickle.load(pickle_in)

# Create Flight Directory
flight_keys=[]

# Get all Flights and File from Data Directory
path, pattern = 'data/', 'ADR'
folders = [path + file for file in os.listdir(path) if pattern in file]

for folder in folders:
    for file in os.listdir(folder):
        flight_keys.append("/{}".format(file.split('.')[0]))

# Setup store
store = pd.HDFStore('bin/flightstore.h5')
print("Loading processed flight keys. This could take some time")
store_keys = store.keys()

print("Loading processed flight statistics. This is {} records".format(len(store_keys)))
pbar = tqdm(total=len(flight_keys))
#df = pd.concat([addindex(store[k],k) for k in store_keys])
df = pd.concat([addindex(store[k],k) for k in flight_keys if store.__contains__(k)])

print("Applying bin filters to all processed flight statistics. This is {} variable columns".format(len(list(df))))
pbar = tqdm(total=len(list(df)))

for var in list(df):
    df[var] = pd.cut(df[var].values, bins=binfilter[var][0], labels=binfilter[var][1])
    pbar.update(1)


df.to_csv("bin/final.csv")