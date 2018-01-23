import os
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Lock
import random
from tqdm import tqdm
import warnings
import gc
import signal


warnings.filterwarnings("ignore")

def tohours(timestr):
    timelist = timestr.split(":")
    return float(timelist[0])+float(timelist[1])/60.0 + float(timelist[2])/(60.0*60.0)

def handler(signum, frame):
    print 'Signal handler called with signal', signum

def init(l):
    global lock
    lock = l

def process(flight):
    try:
        data = {}

        # Load flight data from raw data file
        df = pd.read_csv(flight_directory[flight], index_col=False)

        # Split data into binary and continuos varaible dataFrames
        df_cont = df[[col for col in list(df) if col in cont_vars and col != "TIME"]]
        df_bi = df[[col for col in list(df) if col in bi_vars]]


        # compute descriptive stats for continuos variables
        desc = df_cont.describe(include='all')

        skew = df_cont.skew(axis=0).to_frame().transpose()
        skew.rename(index={0: 'skew'}, inplace=True)

        kur = df_cont.kurtosis(axis=0).to_frame().transpose()
        kur.rename(index={0: 'kurt'}, inplace=True)

        sdf = pd.concat([desc,skew,kur])

        for var in list(sdf):
            for stat in list(sdf.index):
                data["{}_{}".format(var,stat)]=sdf[var].loc[stat]

        # compute counts for binary variables
        df_bi_int = df_bi.diff()
        temp = {}
        for col in list(df_bi_int):
            temp[col] = list(df_bi_int[col]).count(1)

        df_bi_counts = pd.DataFrame(temp,index=[0])

        for var in list(df_bi_counts):
            data["{}_count".format(var)] = df_bi_counts[var].loc[0]

        # take care of special columns
        # time
        start, stop  = df["TIME"].iloc[0], df["TIME"].iloc[-1]
        tof = tohours(stop)-tohours(start) #in hours
        data["TOF_hrs"] = tof

        #take care of min and max flags

        #Make dataFrame with all new statistics
        df_all = pd.DataFrame(data,index =[0])
        df_all_wonan = df_all.dropna(axis=1, how='all')

        #Write to HDF5 Store
        lock.acquire()
        store[flight] = df_all_wonan
        lock.release()
        pbar.update(1)
    except Exception as e:
        os.rename(flight_directory[flight], "bin/fail/{}".format(flight))
    return 0





#################################################################################################################
# Prepare Environment variables
print(gc.collect())
# Setup store
store = pd.HDFStore('bin/flightstore.h5')
print("Loading processed flight keys. This could take some time")
#store_keys = store.keys()


# Load Variable Description File
variables = pd.read_csv("bin/VarDesc.csv")
variables.set_index(['Name'], inplace=True)

cont_vars = variables.index[(variables['process'] == "y") & (variables['Type'] == "c")].tolist()
bi_vars = variables.index[(variables['process'] == "y") & (variables['Type'] == "b")].tolist()

# Create Flight Directory
flight_directory = {}

# Get all Flights and File from Data Directory
path, pattern = 'data/', 'ADR'
folders = [path + file for file in os.listdir(path) if pattern in file]

for folder in folders:
    for file in os.listdir(folder):
        flight_directory[file.split('.')[0]] = folder + '/' + file

#Flights not processed yet
#np_flight_keys = [k for k in flight_directory.keys() if "/{}".format(k) not in store_keys]
np_flight_keys = [k for k in flight_directory.keys() if not store.__contains__("/{}".format(k))]

print("There are {} unprocessed flight files in the {} directory".format(len(np_flight_keys), path))

# Flights to processes
samplesize_perc = 1.0  # percent of all files
samplesize = int((samplesize_perc / 100) * len(np_flight_keys))
flights_sample = random.sample(np_flight_keys, samplesize)
print("You have elected to preprocess {}% of the flight unprocessed files, this is {} files.".format(samplesize_perc, samplesize))

array = [fl for fl in flights_sample if "/{}".format(fl)]

# Prepare progress bar
pbar = tqdm(total=len(array))

#################################################################################################################
# Step 1: Load new Flight file. Select Variables to be processed. Compute Stats on Variables. Store to H5 store

#Prepare Threading pool and lock
#l = Lock()
#pool = ThreadPool(6, initializer=init, initargs=(l,))

# Begin Preprocess Step
#pool.map(process, array)
#pool.close()
#pool.join()

for fl in array:
    process(fl)


