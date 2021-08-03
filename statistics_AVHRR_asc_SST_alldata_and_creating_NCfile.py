#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
#############################################################
#runfile('./classify_AVHRR_asc_SST-01.py', 'daily 0.1 2019', wdir='./MODIS_hdf_Python/')
#cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done
#conda activate MODIS_hdf_Python_env && cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && python classify_AVHRR_asc_SST.py daily 0.01 2011
#conda activate MODIS_hdf_Python_env && cd /mnt/Rdata/RS-data/KOSC/MODIS_hdf_Python/ && python classify_AVHRR_asc_SST.py daily 1.0 2019
'''

from glob import glob
from datetime import datetime
import numpy as np
import netCDF4 as nc
import os
import sys
import MODIS_hdf_utilities

arg_mode = True
arg_mode =  False

log_file = os.path.basename(__file__)[:-3]+".log"
err_log_file = os.path.basename(__file__)[:-3]+"_err.log"
print ("log_file: {}".format(log_file))
print ("err_log_file: {}".format(err_log_file))

if arg_mode == True :
    from sys import argv # input option
    print("argv: {}".format(argv))

    if len(argv) < 4 :
        print ("len(argv) < 2\nPlease input L3_perid and year \n ex) aaa.py daily 0.1 2016")
        sys.exit()
    elif len(argv) > 4 :
        print ("len(argv) > 2\nPlease input L3_perid and year \n ex) aaa.py daily 0.1 2016")
        sys.exit()
    elif argv[1] == 'daily' or argv[1] == 'weekly' or argv[1] == 'monthly' :
        L3_perid, resolution, year = argv[1], float(argv[2]), int(argv[3])
        print("{}, {}, {} processing started...".format(argv[1], argv[2], argv[3]))
    else :
        print("Please input L3_perid and year \n ex) aaa.py daily 0.1 2016")
        sys.exit()
else :
    L3_perid = 'daily'
    resolution = 1.0
    year = 2019
    
# Set Datafield name
DATAFIELD_NAME = "AVHRR_SST"

#Set lon, lat, resolution
Llon, Rlon = 115, 145
Slat, Nlat = 20, 55
#L3_perid, resolution, yr = "daily", 0.1, 2019

#set directory
base_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution), L3_perid)
#save_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
#                                                        str(Slat), str(Nlat), str(resolution), L3_perid)

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*alldata.npy')))

print("len(fullnames): {}".format(len(fullnames)))

for fullname in fullnames : 
    #fullname = fullnames[0]
    print("Starting {0}\n".format(fullname))
    fullname_el = fullname.split("/")
    filename_el = fullname_el[-1].split("_")
    #if os.path.exists('{0}_mean.npy'.format(fullname[:-4])) :
    #    print('{0}_mean.npy is already exist...'.format(fullname[:-4]))
    
    if os.path.exists('{0}_mean.nc'.format(fullname[:-4])) :
        print('{0}_mean.nc is already exist...'.format(fullname[:-4]))
    
    else : 
        
        alldata = np.load(fullname, allow_pickle=True)
        ds = nc.Dataset('{0}_mean.nc'.format(fullname[:-4]), 'w', format='NETCDF4')
        
        #time = ds.createDimension('time', filename_el[2])
        time = ds.createDimension('time', None)
        
        lon = ds.createDimension('longitude', alldata.shape[0])
        lat = ds.createDimension('latitude', alldata.shape[1])
        times = ds.createVariable('time', 'f4', ('time',))
        
        lons = ds.createVariable('longitude', 'f4', ('longitude',))
        lats = ds.createVariable('latitude', 'f4', ('latitude',))
        SST = ds.createVariable('SST', 'f4', ('time', 'latitude', 'longitude',))
        SST.units = 'degree'
        
        lons[:] = np.arange(Llon, Rlon+resolution, resolution)
        lats[:] = np.arange(Slat, Nlat+resolution, resolution)
        
        alldata = alldata.transpose()
        for i in range(alldata.shape[0]):
            for j in range(alldata.shape[1]):
                if len(alldata[i,j]) == 0 : 
                    alldata[i,j] = np.nan
                else : 
                    alldata[i,j] = np.mean(list(map(lambda x:x[1], alldata[i,j])))
                    #for each_data in alldata[i,j] :
                        #np.mean(list(map(lambda x:x[1], eachdata)))
        SST[0, :, :] = alldata.transpose()
        
        #print('var size after adding first data', value.shape)
        #xval = np.linspace(0.5, 5.0, alldata.shape[1]-1)
        #yval = np.linspace(0.5, 5.0, alldata.shape[0]-1)
        #value[1, :, :] = np.array(xval.reshape(-1, 1) + yval)

        ds.close()
        