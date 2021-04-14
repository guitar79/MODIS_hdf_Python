#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
Created on Wed Oct 21 14:38:31 2020
@author: guitar79
created by Kevin
#Open hdf file
NameError: name 'SD' is not defined
conda install -c conda-forge pyhdf

runfile('./classify_AVHRR_asc_SST-01.py', 'daily 0.1 2019', wdir='./MODIS_hdf_Python/')

len(npy_data[795,183])
np.mean(npy_data[795,183])

hdf_data = np.load(f_name1, allow_pickle=True)

import numpy as np
import matplotlib.pyplot as plt

plt.title("Histogram of {}".format(fullname), fontsize=9)
plt.hist(hdf_value)

plt.grid(True)
plt.show()
#plt.show()

cd '/mnt/14TB1/RS-data/KOSC/MODIS_hdf_Python' && for yr in {2011..2020}; do python classify_AVHRR_asc_SST-01.py daily 0.05 $yr; done


'''

from glob import glob
from datetime import datetime
import numpy as np
import os
import sys
from sys import argv # input option
import MODIS_hdf_utilities

'''
print("argv: {}".format(argv))

if len(argv) < 4 :
    print ("len(argv) < 2\nPlease input L3_perid and year \n ex) aaa.py daily 0.1 2016")
    sys.exit()
elif len(argv) > 4 :
    print ("len(argv) > 2\nPlease input L3_perid and year \n ex) aaa.py daily 0.1 2016")
    sys.exit()
elif argv[1] == 'daily' or argv[1] == 'weekly' or argv[1] == 'monthly' :
    L3_perid, resolution, yr = argv[1], float(argv[2]), int(argv[3])
    print("{}, {}, {} processing started...".format(argv[1], argv[2], argv[3]))
else :
    print("Please input L3_perid and year \n ex) aaa.py daily 0.1 2016")
    sys.exit()
'''
L3_perid = 'daily'
resolution = 0.5
yr = 2015
    
# Set Datafield name
DATAFIELD_NAME = "AVHRR_SST"

#Set lon, lat, resolution
Llon, Rlon = 110, 150
Slat, Nlat = 15, 55

#L3_perid, resolution, yr = "daily", 0.1, 2019

# long file option
add_log = True
if add_log == True :
    log_file = "AVHRR_{}_python.log".format(DATAFIELD_NAME)
    err_log_file = "AVHRR_{}_python_err.log".format(DATAFIELD_NAME)

#set directory
base_dir_name = '../L2_AVHRR_SST/'
save_dir_name = "../L3_{0}/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                        str(Slat), str(Nlat), str(resolution), L3_perid)

if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)
    print ('*'*80)
    print (save_dir_name, 'is created')
else :
    print ('*'*80)
    print (save_dir_name, 'is exist')

years = range(yr, yr+1)


proc_dates = []

#make processing period tuple
for year in years:
    dir_name = base_dir_name + str(year) + '/'

    from dateutil.relativedelta import relativedelta
    s_start_date = datetime(year, 1, 1) #convert startdate to date type
    s_end_date = datetime(year+1, 1, 1)

    k=0
    date1 = s_start_date
    date2 = s_start_date
    
    while date2 < s_end_date :
        k += 1
        if L3_perid == 'daily' :
            date2 = date1 + relativedelta(days=1)
        elif L3_perid == 'weekly' :
            date2 = date1 + relativedelta(days=8)
        elif L3_perid == 'monthly' :
            date2 = date1 + relativedelta(months=1)

        date = (date1, date2, k)
        proc_dates.append(date)
        date1 = date2

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*.asc')))

fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utilities.fullname_to_datetime_for_KOSC_AVHRR_SST_asc(fullname))

import pandas as pd 

len(fullnames)
len(fullnames_dt)

# Calling DataFrame constructor on list 
df = pd.DataFrame({'fullname':fullnames,'fullname_dt':fullnames_dt})
df.index = df['fullname_dt']
df

for proc_date in proc_dates[:]:

    df_proc = df[(df['fullname_dt'] >= proc_date[0]) & (df['fullname_dt'] < proc_date[1])]
    
    #check file exist??
    if os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy'\
            .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))\
        and os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt'\
            .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) :
            
        print(('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are exist...'\
            .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
            str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))))
    
    else : 

        if len(df_proc) == 0 :
            print("There is no data in {0} - {1} ...\n"\
                  .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
        
        else :                
            print("df_proc: {}".format(df_proc))
        
            processing_log = "#This file is created using Python : https://github.com/guitar79/MODIS_hdf_Python\n"
            processing_log += "#L3_perid = {}, start date = {}, end date = {}\n"\
                .format(L3_perid, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'))
    
            processing_log += "#Llon = {}, Rlon = {}, Slat = {}, Nlat = {}, resolution = {}\n"\
                .format(str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))
            
            # make array_data
            print("{0}-{1} Start making grid arrays...\n".\
                  format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
            array_data = MODIS_hdf_utilities.make_grid_array(Llon, Rlon, Slat, Nlat, resolution)
            print('Grid arrays are created...........\n')
        
            total_data_cnt = 0
            file_no = 0
            processing_log += "#processing file list\n"
            processing_log += "#file No, data_count, filename, mean(sst), max(sst), min(sst), \
                            min(longitude), max(longitude), min(latitude), max(latitude)\n"
            array_alldata = array_data.copy()
            
            for fullname in df_proc["fullname"] : 
                fullname_el = fullname.split("/")
                print("Reading hdf file {0}\n".format(fullname))
                df_AVHRR_sst = pd.read_table("{}".format(fullname), sep='\t', header=None, index_col=0,
                                   names = ['index', 'latitude', 'longitude', 'sst'],
                                   engine='python')
                df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.sst == "***"].index)
                df_AVHRR_sst["sst"] = df_AVHRR_sst.sst.astype("float16")
                df_AVHRR_sst["longitude"] = df_AVHRR_sst.longitude.astype("float16")
                df_AVHRR_sst["latitude"] = df_AVHRR_sst.latitude.astype("float16")
                print("df_AVHRR_sst : {}".format(df_AVHRR_sst))
                
                #check dimension    
                if len(df_AVHRR_sst) == 0 :
                    print("There is no sst data...")
                        
                else :
                    df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.longitude < Llon].index)
                    df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.longitude > Rlon].index)
                    df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.latitude > Nlat].index)
                    df_AVHRR_sst = df_AVHRR_sst.drop(df_AVHRR_sst[df_AVHRR_sst.latitude < Slat].index)
                    
                    df_AVHRR_sst["lon_cood"] = (((df_AVHRR_sst["longitude"]-Llon)/resolution*100)//100)
                    df_AVHRR_sst["lat_cood"] = (((Nlat-df_AVHRR_sst["latitude"])/resolution*100)//100)
                    df_AVHRR_sst["lon_cood"] = df_AVHRR_sst.lon_cood.astype("int16")
                    df_AVHRR_sst["lat_cood"] = df_AVHRR_sst.lat_cood.astype("int16")
                    
                    if os.path.exists("{0}{1}_{2}_hist.png"\
                        .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
            
                        print("{0}{1}_{2}_hist.png is already exist..."\
                              .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                    else : 
                        plt_hist = MODIS_hdf_utilities.draw_histogram_AVHRR_SST_asc(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME)
                        
                        plt_hist.savefig("{0}{1}_{2}_hist.png"\
                            .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                        print("{0}{1}_{2}_hist.png is created..."\
                            .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                        plt_hist.close()
                                        if os.path.exists("{0}{1}_{2}_hist.png"\
                        .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME)) :
            
                        print("{0}{1}_{2}_hist.png is already exist..."\
                              .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                    else : 
                        plt_hist = MODIS_hdf_utilities.draw_histogram_AVHRR_SST_asc(df_AVHRR_sst, save_dir_name, fullname, DATAFIELD_NAME)
                        
                        plt_hist.savefig("{0}{1}_{2}_hist.png"\
                            .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                        print("{0}{1}_{2}_hist.png is created..."\
                            .format(save_dir_name, fullname_el[-1][:-4], DATAFIELD_NAME))
                        plt_hist.close()
                    data_cnt = 0
                    NaN_cnt = 0
                    
                    for index, row in df_AVHRR_sst.iterrows():
                        data_cnt += 1
                        #array_alldata[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                        array_alldata[df_AVHRR_sst.lon_cood[index]][df_AVHRR_sst.lat_cood[index]].append((fullname_el[-1], df_AVHRR_sst.sst[index]))
                                
                        print("array_alldata[{}][{}].append({}, {})"\
                              .format(df_AVHRR_sst.lon_cood[index], df_AVHRR_sst.lat_cood[index], fullname_el[-1], df_AVHRR_sst.sst[index]))
                        print("{} data added...".format(data_cnt))
                    
                    file_no += 1
                    total_data_cnt += data_cnt

                    processing_log += "{0}, {1}, {2}, {3:.02f}, {4:.02f}, {5:.02f}, {6:.02f}, {7:.02f}, {8:.02f}, {9:.02f}\n"\
                        .format(str(file_no), str(data_cnt), str(fullname),  \
                                np.nanmean(df_AVHRR_sst["sst"]), np.nanmax(df_AVHRR_sst["sst"]), np.nanmin(df_AVHRR_sst["sst"]),
                                np.nanmin(df_AVHRR_sst["longitude"]), np.nanmax(df_AVHRR_sst["longitude"]),\
                                np.nanmin(df_AVHRR_sst["latitude"]), np.nanmax(df_AVHRR_sst["latitude"]))

                                
            processing_log += '#total data number =' + str(total_data_cnt) + '\n'
            
            #print("array_alldata: {}".format(array_alldata))
            print("prodessing_log: {}".format(processing_log))
                                        
            np.save('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_alldata.npy'\
                .format(save_dir_name, DATAFIELD_NAME, 
                proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), array_alldata)
            
            with open('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt'\
                  .format(save_dir_name, DATAFIELD_NAME, \
                  proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), \
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
                f.write(processing_log)

            print('#'*60)
            MODIS_hdf_utilities.write_log(log_file, \
                '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are is created.'\
                .format(save_dir_name, DATAFIELD_NAME, \
                proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), \
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))
