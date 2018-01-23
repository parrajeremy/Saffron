import pickle

pickle_in = open("bin/bins.pickle","rb")
binfilter = pickle.load(pickle_in)
pickle_in = open("bin/bins_mod.pickle","rb")
binfilter_wmod = pickle.load(pickle_in)

print(binfilter_wmod['AOA01_mean'])