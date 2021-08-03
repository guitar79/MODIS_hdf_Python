
from glob import glob
from datetime import datetime
import numpy as np
import netCDF4 as nc
import os
import sys

from netCDF4 import Dataset as NetCDFFile 
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap

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
save_dir_name = base_dir_name
#save_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
#                                                        str(Slat), str(Nlat), str(resolution), L3_perid)

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*mean.nc')))

print("len(fullnames): {}".format(len(fullnames)))

for fullname in fullnames : 
    #fullname = fullnames[0]
    print("Starting {0}\n".format(fullname))
    fullname_el = fullname.split("/")
    filename_el = fullname_el[-1].split("_")
    #if os.path.exists('{0}_mean.npy'.format(fullname[:-4])) :
    #    print('{0}_mean.npy is already exist...'.format(fullname[:-4]))
    
    if os.path.exists('{0}_mean_map.png'.format(fullname[:-3])) :
        print('{0}_mean_map.png is already exist'.format(fullname[:-3]))
    
    else : 
        
        nc_data = NetCDFFile(fullname) # note this file is 2.5 degree, so low resolution data
        lat = nc_data.variables['latitude'][:]
        lon = nc_data.variables['longitude'][:]
        time = nc_data.variables['time'][:]
        SST = nc_data.variables['SST'][:] # SST
        
        plt_map = MODIS_hdf_utilities.draw_map_SST_nc(SST, lon, lat, save_dir_name, fullname, DATAFIELD_NAME, Llon, Rlon, Slat, Nlat)
        
        plt_map.savefig('{0}_mean_map.png'.format(fullname[:-3]))
        print('{0}_mean_map.png is created...'.format(fullname[:-3]))
        plt_map.close()
        
    