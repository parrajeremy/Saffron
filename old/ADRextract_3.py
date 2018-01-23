import pandas as pd
import numpy as np
import os
import time
import random
from tqdm import tqdm
import warnings
import csv
import sys
from collections import defaultdict

warnings.filterwarnings("ignore")

class NAVAIR:
    def __init__(self):
        self.flightfile_store = pd.HDFStore('store.h5')
        self.flightfile_store_keys = self.flightfile_store.keys()
        self.vars_df = pd.read_csv("VarDesc.csv", index_col=0)

        #self.vars = list(self.vars_df.index.values)
        #self.newvariables=False

    def makebins(self, samplesize='all'):
        if samplesize == 'all':
            self.sample_flight_keys_tobin = self.sample_flight_keys
        else:
            self.sample_flight_keys_tobin = random.sample(self.sample_flight_keys, samplesize)


    def update_varfile(self,var):
        temp_df = pd.DataFrame(columns=list(self.vars_df), index=[var])
        self.vars_df = self.vars_df.append(temp_df)

        print(self.vars_df.index.values)
        self.vars_df.to_csv("VarDesc.csv")
        self.vars_df = pd.read_csv("VarDesc.csv", index_col=0)
        self.vars = list(self.vars_df.index.values)
        self.newvariables=True

    def injest(self, flight, file):
        df = pd.read_csv(file)



        # Check that all variables have asscoiated parameters
        #for var in list(df):
        #    if var not in self.vars:
        #       self.update_varfile(var)


        # collect statistics on cont. variables
        desc = df.describe(include='all')

        skew = df.skew(axis=0).to_frame().transpose()
        skew.rename(index={0:'skew'},inplace=True)

        kur = df.kurtosis(axis=1).to_frame().transpose()
        kur.rename(index={0: 'kurt'}, inplace=True)

        stats = pd.concat([desc,skew,kur])
        stats.drop(columns=['TIME'], inplace=True)

        #collect stats on binaray variables

        #write stats to file
        self.writetostore(flight, stats)
        return stats


    def cont_stats(self,df):
        desc = df.describe(include='all')
        skew = df.skew(axis=0).to_frame().transpose()
        skew.rename(index={0:'skew'},inplace=True)
        kur = df.kurtosis(axis=1).to_frame().transpose()
        kur.rename(index={0: 'kurt'}, inplace=True)
        stats = pd.concat([desc,skew,kur])

        return stats
    def writetostore(self,flight,stats):
        # write flight stats to pytable
        self.flightfile_store[flight] = stats


    def loadpanel(self):
        self.pdata =pd.Panel(dict((flight,self.readfromstore(flight)) for flight in self.flightfile_store_keys))


    def readfromstore(self,flight):
        return self.store.get(flight)





def getfolders(path,pattern):
    folders = [path+file for file in os.listdir(path) if pattern in file]

    return folders

def getflightfiles(folders):
    flights = {}
    for folder in folders:
        for file in os.listdir(folder):
            flights[file.split('.')[0]] = folder + '/' + file

    return flights





def main():
    # Where does the data live?
    path = 'data/'
    pattern = 'ADR'
    folders = getfolders(path, pattern)
    ffiles = getflightfiles(folders)
    ffiles_keys = ffiles.keys()

    # Are we going to process the entire batch?
    samplesize = 10.0 #percent of all files
    ffiles_sample_keys = random.sample(ffiles_keys, int((samplesize/100)*len(ffiles_keys)))

    # Select a percentatge of batch size to be used to create bin ranges. 100% would mean the entire set was used.
    bin_samplesize   = 10.0  #percent of samplesize files

    # Prepare Progress bar
    pbar_total = int((samplesize/100*(1+bin_samplesize/100))* len(ffiles_keys))
    pbar = tqdm(total=pbar_total)
    print(pbar_total)

    #error counter
    error_cnt = 0

    #Flight Instance
    f = NAVAIR()

    # load Store keys

    #Begin Progress loop, Update pbar to add to progress status
    while 1:

        # Compute all flight Stats and store
        print("Ingesting Flight File Data")
        for flight in ffiles_sample_keys:
            if flight not in f.flightfile_store_keys:
                #try:

                    f.injest(flight, ffiles[flight])
                    f.flightfile_store_keys.append(flight)

                #except IOError as (errno, strerror):
                #    print("I/O error({0}): {1}".format(errno, strerror))
                #except ValueError:
                #    print("Could not convert data to an integer.")
                #except:
                #    print("Unexpected error:", sys.exc_info()[0])
                    #raise
            if error_cnt > 0.5 * samplesize:
                print("The number of errors has exeeded the maximum limit set. Quiting now")
                break
            pbar.update(1)  # Update progress bar

        # Check for new variables
        #if f.newvariables:
        #    print("You have new variables, please update variable description file and restart")
        #    break


        # Select Random Sample of Flight Stats and generate bins (names and values)
        #pan = f.sampletopanel()
        #print(pan.ix[:,'mean',:])

        #for flight in f.sample_flight_keys_tobin:
        #    print(f.readfromstore(flight))
        #    pbar.update(1)

        # Open all flights re-label states with bin names. Store to CSV file for export to Saffron
        f.flightfile_store.flush()
        f.flightfile_store.close()

        break




if __name__ == "__main__":
    main()


#columns = ['P3','NH','NL','EGT','FF','PLA','Hp','WARN']
#rang = {'P3':'stats','NH':[0,115],'NL':[0-120],'EGT':[0,1350],'FF':[0,116.66],'PLA':[0-105],'Hp':[-1000,53000],'WARN':'bool'}
#rang = {'P300':'stats','NH00':'stats','NL00':'stats','EGT_D00':'stats','FF00':'stats','PLA00':'stats','Hp00':'stats','WARN00':'bool'}