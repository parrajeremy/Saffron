import os
from shutil import copyfile
import random
import string

NAtoy_path = 'C:/Users/Saffron4/PycharmProjects/NAVAIR_toy/data/'
NA_path = 'C:/Users/Saffron4/PycharmProjects/NAVAIR/data/'

def NA_mkdir(path,dirs):
    for d in dirs:
        dir = path + d
        if not os.path.exists(dir):
            os.makedirs(dir)

def createfiles(orgPath,newPath,folder,num):
    files =os.listdir(orgPath+folder)
    for f in files[:num]:
        newfilename = ''.join(random.choices(string.ascii_uppercase + string.digits, k=len(f)-4))+'.csv'
        print(newfilename)
        copyfile(orgPath+folder+'/'+f,newPath+folder+'/'+newfilename)

folders = os.listdir(NA_path)
NA_mkdir(NAtoy_path,folders)

for f in folders:
    createfiles(NA_path,NAtoy_path,f,1000)