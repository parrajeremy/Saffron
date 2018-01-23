import pandas as pd
import os
import time
import random
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

class flights:
    def __init__(self):
        self.folders = []
        self.pathtodata='data/'
        self.folderpattern = 'ADR'
        self.folders = []
        self.flights = {}
        self.flight_keys=[]
        self.sample_flight_keys=[]
        self.sample_flight_keys_tobin=[]
        self.store = pd.HDFStore('store.h5')
        self.storekeys = self.store.keys()

    def sampleflightstostore(self,samplesize='all'):
        self.flight_keys=self.flights.keys()
        if samplesize =='all':
            self.sample_flight_keys = self.flight_keys
        else:
            self.sample_flight_keys = random.sample(self.flight_keys, samplesize)

    def sampleflightstobin(self, samplesize='all'):
        if samplesize == 'all':
            self.sample_flight_keys_tobin = self.sample_flight_keys
        else:
            self.sample_flight_keys_tobin = random.sample(self.sample_flight_keys, samplesize)

    def getfolders(self):
        path = self.pathtodata
        pattern = self.folderpattern

        self.folders = [path+f for f in os.listdir(path) if pattern in f]


    def getflights(self):
        for f in self.folders:
            for file in os.listdir(f):
                self.flights[file.split('.')[0]] = f+ '/' + file


    def getstats(self,flight, file):
        # collect statitsics on flight file
        df = pd.read_csv(file)

        desc = df.describe(include='all')

        skew = df.skew(axis=0).to_frame().transpose()
        skew.rename(index={0:'skew'},inplace=True)

        kur = df.kurtosis(axis=1).to_frame().transpose()
        kur.rename(index={0: 'kurt'}, inplace=True)

        stats = pd.concat([desc,skew,kur])
        stats.drop(columns=['TIME'], inplace=True)

        #print(list(stats.index))
        #stats.set_index([0],inplace=True)
        self.writetostore(flight, stats)
        return stats


    def writetostore(self,flight,stats):
        # write flight stats to pytable
        self.store[flight] = stats


    def sampletopanel(self):
        pdata =pd.Panel(dict((flight,self.readfromstore(flight)) for flight in self.sample_flight_keys))

        return pdata

    def readfromstore(self,flight):
        return self.store.get(flight)





def getfolders(path,pattern):
    folders = [path+f for f in os.listdir(path) if pattern in f]

    return folders

def getflightfiles(folders):
    for f in folders:
        for file in os.listdir(f):
            flights[file.split('.')[0]] = f+ '/' + file

    return flights


#columns = ['P3','NH','NL','EGT','FF','PLA','Hp','WARN']
#rang = {'P3':'stats','NH':[0,115],'NL':[0-120],'EGT':[0,1350],'FF':[0,116.66],'PLA':[0-105],'Hp':[-1000,53000],'WARN':'bool'}
#rang = {'P300':'stats','NH00':'stats','NL00':'stats','EGT_D00':'stats','FF00':'stats','PLA00':'stats','Hp00':'stats','WARN00':'bool'}


def main():

    path = 'data/'
    pattern = 'ADR'
    folders  = getfolders(path, pattern)
    ffiles = getflightfiles(folders)
    ffiles_keys = ffiles.keys()

    samplesize = 100 #percent of all files
    ffiles_sample_keys = random.sample(ffiles_keys, int(samplesize/100)*len(ffiles_keys))


    binflights_samplesize   = 10  #percent of samplesize files

    # Prepare Progress bar
    pbar_total = len(ffiles_sample_keys)
    pbar = tqdm(total=pbar_total)

    f = flights()

    while  1:
        # Compute all flight Stats and store
        print("Ingesting Flight File Data"
              )
        for flight in ffiles_sample_keys:
            if flight not in f.storekeys:
                f.getstats(flight, ffiles[flight])
            pbar.update(1)

        # Select Random Sample of Flight Stats and generate bins (names and values)
        pan = f.sampletopanel()
        #print(pan.ix[:,'mean',:])

        #for flight in f.sample_flight_keys_tobin:
        #    print(f.readfromstore(flight))
        #    pbar.update(1)

        # Open all flights re-label states with bin names. Store to CSV file for export to Saffron


        break






#for p in paths:
#    filenames = [p+f for f in os.listdir(p)]
#print(filenames[:2])
#cols = [col for col in rang.keys()] #if rang[col] == 'stats']
#for f in filenames[:500]:
#    df = pd.read_csv(f)
#    try:
#        df2 = df[cols]
#        print(stats(df2))
#    except KeyError:
#        pass