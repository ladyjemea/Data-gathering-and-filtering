#pylint: disable=all
#pylint: disable=no-member
import pandas as pd
import numpy as np
import subprocess
from pandas import read_csv
from csv import reader
import datetime as dtm
import time
from datetime import timezone, datetime
import os
import sys 
import json


yr = dtm.datetime.utcnow().year
mo = dtm.datetime.utcnow().month
today =dtm.datetime.utcnow().day
next_day = (dtm.datetime.utcnow() + dtm.timedelta(days=1)).day
time = dtm.datetime.utcnow()
hour = time.hour
minute = time.minute
date = dtm.datetime(year=yr, month=mo, day=today, hour=hour, minute=minute)
wk = date.isocalendar()[1]


''' Collects data from all data sources needed for creating the products; filters them, writes the output to the various csv/excel files and calls the scripts which make the plots'''

date = dtm.datetime.utcnow()

year = dtm.datetime.strftime(date, '%Y')
month = dtm.datetime.strftime(date, '%m')
day = dtm.datetime.strftime(date, '%d')

url_tro = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=tro2a&year="+year+"&month="+month+"&day="+day+"&res=60sec&pwd=&format=html&comps=DHZ&getdata=+Get+Data+"
dfTro = pd.read_csv(url_tro, skiprows = 7)

url_dob = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=dob1a&year="+year+"&month="+month+"&day="+day+"&res=60sec&pwd=&format=html&comps=DHZ&getdata=+Get+Data+"
dfDob = pd.read_csv(url_dob, skiprows = 7)

url_nal = "http://flux.phys.uit.no/cgi-bin/mkascii.cgi?site=nal1a&year="+year+"&month="+month+"&day="+day+"&res=60sec&pwd=&format=html&comps=DHZ&getdata=+Get+Data+"
dfNal = pd.read_csv(url_nal, skiprows = 7)


dfS = pd.read_json("https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json")
dfB = pd.read_json("https://services.swpc.noaa.gov/products/solar-wind/mag-7-day.json")
dfX = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/xrays-7-day.json")
dfP = pd.read_json("https://services.swpc.noaa.gov/json/goes/primary/integral-protons-7-day.json")



'''Data processing'''

#MAGNRTOMETER TROMSØ
dfTro = dfTro.astype(str)
dfTro = dfTro[dfTro.columns[0]].str.split(expand=True)

dfTro.columns = ['Date', 'Time', 'Dec', 'Horiz', 'Vert', 'Incl', 'Tot']
dfTro['timestamp'] = dfTro['Date'] + ' ' + dfTro['Time']
dfTro.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfTro = dfTro[dfTro.Horiz != '99999.9']
dfTro.rename(columns={'Horiz':'Horiz_tro'}, inplace=True)
dfTro['timestamp'] = pd.to_datetime(dfTro['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfTro.set_index('timestamp', inplace=True)

#MAGNETOMETER DATA FOR AUTOMATED ALERTS TROMSØ
dfTro.to_csv('data/threshold_mag_tro.csv', index= True, sep= " ")

with open('data/magnetometer_tro.csv', 'a')as f:
    if os.path.getsize('data/magnetometer_tro.csv') <= 0:
        dfTro.to_csv(f, header=f.tell()==0, index = True, sep = " ")
    elif os.path.getsize('data/magnetometer_tro.csv') > 0:
        df1 = pd.read_csv('data/magnetometer_tro.csv', sep = " ")
        df1['timestamp'] = pd.to_datetime(df1['timestamp'])
        df1.set_index('timestamp', inplace=True)
        if dtm.datetime.utcnow() > df1.index[-2]:
            values = dfTro.index > df1.index[-2]
            df1 = dfTro[values]
            df1.to_csv('data/magnetometer_tro.csv', mode='a', index =True, sep = " ", header = False)
f.close()



#MAGNRTOMETER DOMBÅS
dfDob = dfDob.astype(str)
dfDob = dfDob[dfDob.columns[0]].str.split(expand=True)

dfDob.columns = ['Date', 'Time', 'Dec', 'Horiz', 'Vert', 'Incl', 'Tot']
dfDob['timestamp'] = dfDob['Date'] + ' ' + dfDob['Time']
dfDob.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfDob = dfDob[dfDob.Horiz != '99999.9']
dfDob.rename(columns={'Horiz':'Horiz_dob'}, inplace=True)
dfDob['timestamp'] = pd.to_datetime(dfDob['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfDob.set_index('timestamp', inplace=True)

#MAGNETOMETER DATA FOR AUTOMATED ALERTS DOMBÅS
dfDob.to_csv('data/threshold_mag_dob.csv', index= True, sep= " ")

with open('data/magnetometer_dob.csv', 'a')as f:
    if os.path.getsize('data/magnetometer_dob.csv') <= 0:
        dfDob.to_csv(f, header=f.tell()==0, index = True, sep = " ")
    elif os.path.getsize('data/magnetometer_dob.csv') > 0:
        df1 = pd.read_csv('data/magnetometer_dob.csv', sep = " ")
        df1['timestamp'] = pd.to_datetime(df1['timestamp'])
        df1.set_index('timestamp', inplace=True)
        if dtm.datetime.utcnow() > df1.index[-2]:
            values = dfDob.index > df1.index[-2]
            df1 = dfDob[values]
            df1.to_csv('data/magnetometer_dob.csv', mode='a', index =True, sep = " ", header = False)
f.close()



#MAGNRTOMETER NY ÅLESUND
dfNal = dfNal.astype(str)
dfNal = dfNal[dfNal.columns[0]].str.split(expand=True)

dfNal.columns = ['Date', 'Time', 'Dec', 'Horiz', 'Vert', 'Incl', 'Tot']
dfNal['timestamp'] = dfNal['Date'] + ' ' + dfNal['Time']
dfNal.drop(['Date', 'Time', 'Dec', 'Vert', 'Incl', 'Tot'], axis = 1, inplace = True)
dfNal = dfNal[dfNal.Horiz != '99999.9']
dfNal.rename(columns={'Horiz':'Horiz_nal'}, inplace=True)
dfNal['timestamp'] = pd.to_datetime(dfNal['timestamp'], format = '%d/%m/%Y %H:%M:%S')
dfNal.set_index('timestamp', inplace=True)

#MAGNETOMETER DATA FOR AUTOMATED ALERTS NY-ÅLESUND
dfNal.to_csv('data/threshold_mag_nal.csv', index= True, sep= " ")

with open('data/magnetometer_nal.csv', 'a')as f:
    if os.path.getsize('data/magnetometer_nal.csv') <= 0:
        dfNal.to_csv(f, header=f.tell()==0, index = True, sep = " ")
    elif os.path.getsize('data/magnetometer_nal.csv') > 0:
        df1 = pd.read_csv('data/magnetometer_nal.csv', sep = " ")
        df1['timestamp'] = pd.to_datetime(df1['timestamp'])
        df1.set_index('timestamp', inplace=True)
        if dtm.datetime.utcnow() > df1.index[-2]:
            values = dfNal.index > df1.index[-2]
            df1 = dfNal[values]
            df1.to_csv('data/magnetometer_nal.csv', mode='a', index =True, sep = " ", header = False)
f.close()



#SOLAR WIND SPEED
dfS.drop([1, 3], axis = 1, inplace = True)
dfS.columns = [''] * len(dfS.columns)
dfS = dfS.drop(dfS.index[0])
dfS.reset_index(drop=True, inplace=True)
dfS.columns = ['timestamp', 'Speed']
dfS['timestamp'] = pd.to_datetime(dfS['timestamp'], format = '%Y-%m-%d %H:%M:%S')
dfS.set_index('timestamp', inplace=True)

with open('data/solar_wind_speed.csv', 'w')as f:
    dfS.to_csv(f, header=f.tell()==0, index =True, sep = " ")
f.close()




#SOLAR WIND BZ
dfB.drop([1, 2, 4, 5, 6], axis = 1, inplace = True)
dfB.columns = [''] * len(dfB.columns)
dfB = dfB.drop(dfB.index[0])
dfB.reset_index(drop=True, inplace=True)
dfB.columns = ['timestamp', 'Bz_gsm']
dfB['timestamp'] = pd.to_datetime(dfB['timestamp'], format = '%Y-%m-%d %H:%M:%S')
dfB.set_index('timestamp', inplace=True)


with open('data/solar_wind_bz.csv', 'w')as f:
    dfB.to_csv(f, header=f.tell()==0, index =True, sep = " ")
f.close()




#Xrays for Weekly mailing
dfX["timestamp"] = pd.to_datetime(dfX["time_tag"], format="%Y-%m-%dT%H:%M:%SZ")
dfX.drop(['time_tag'], axis = 1, inplace = True)
dfX.set_index('timestamp', inplace=True)

dfX.to_csv('data/weekly_xray_protons/xrays_week_' + str(wk) + '.csv', index=True, header=True, sep=" ")
dfX.to_excel('data/weekly_xray_protons/xrays_week_' + str(wk) + '.xlsx', index = True, header=True)


#XRAYS
dfX = dfX[dfX['energy'].str.contains("0.1-0.8nm")]
#dfX["timestamp"] = pd.to_datetime(dfX["time_tag"], format="%Y-%m-%dT%H:%M:%SZ")
dfX.drop(['satellite', 'energy'], axis = 1, inplace = True)
dfX['flux'] =[float('{:.3g}'.format(x)) for x in dfX['flux']]
dfX.rename(columns={'flux':'Xrays'}, inplace=True)
#dfX.set_index('timestamp', inplace=True)


#Xrays for automated alerts
dfX1 = dfX.copy()
dfX1['datetime'] = dfX1.index
dfX1 = dfX1.tail(1)
dfX1.to_csv('data/threshold_xrays.csv', index= True, sep= " ")

with open('data/xrays.csv', 'w')as f:
    dfX.to_csv(f, header=f.tell()==0, index =True, sep = " ")
f.close()


#Xrays for Solar Wind Plots
dfX2 = dfX1.copy()
dfX2.drop(['datetime'], axis = 1, inplace = True)
dfX2.reset_index(drop=True, inplace=True)





#Protons for Weekly mailing
dfP["timestamp"] = pd.to_datetime(dfP["time_tag"], format="%Y-%m-%dT%H:%M:%SZ")
dfP.drop(['time_tag'], axis = 1, inplace = True)
dfP.set_index('timestamp', inplace=True)

dfP.to_csv('data/weekly_xray_protons/protons_week_' + str(wk) + '.csv', index=True, header=True, sep=" ")
dfP.to_excel('data/weekly_xray_protons/protons_week_' + str(wk) + '.xlsx', index = True, header=True)


#PROTONS
dfP = dfP[dfP['energy'].str.contains(">=10 MeV")]
#dfP["timestamp"] = pd.to_datetime(dfP["time_tag"], format="%Y-%m-%dT%H:%M:%SZ")
dfP.drop(['satellite', 'energy'], axis = 1, inplace = True)
dfP['flux'] = dfP['flux'].astype(float).round(2)
dfP.rename(columns={'flux':'Protons'}, inplace=True)
#dfP.set_index('timestamp', inplace=True)


#Protons for automated alerts
dfP1 = dfP.copy()
dfP1['datetime'] = dfP1.index
dfP1 = dfP1.tail(1)
dfP1.to_csv('data/threshold_protons.csv', index= True, sep= " ")

with open('data/protons.csv', 'w')as f:
    dfP.to_csv(f, header=f.tell()==0, index =True, sep = " ")
f.close()


#Protons for Solar Wind Plots
dfP2 = dfP1.copy()
dfP2.drop(['datetime'], axis = 1, inplace = True)
dfP2.reset_index(drop=True, inplace=True)

#Create a dataframe for Xrays and Protons
dfSW = pd.concat( [dfX2, dfP2], axis=1)
with open('data/Solar_wind_xp.csv', 'a')as f:    #write to a csv file each time the script is run
    dfSW.to_csv(f, header=f.tell()==0, index =False, sep = " ")  







'''Calculations for Activity Index'''

#ACTIVITY INDEX TROMSØ
dfTro = pd.read_csv('data/magnetometer_tro.csv', sep = " ")
dfTro = dfTro.drop_duplicates(subset=['timestamp'])

dfTro['timestamp'] = pd.to_datetime(dfTro['timestamp'])
dfTro.set_index('timestamp', inplace=True)

length_tro = len(dfTro.index)-1
for i in range(length_tro):
    if (dfTro.index[i+1]-dfTro.index[i]).seconds >= 300:
        dfTro.loc[dfTro.index[i]+dtm.timedelta(minutes=5)] = float("nan")
dfTro.sort_index(inplace=True)

dfTro_ai = dfTro.resample('H')['Horiz_tro'].agg(['max', 'min'])
dfTro_ai['diff'] = dfTro_ai['max'].sub(dfTro_ai['min'], axis = 0)
dfTro_ai['diff'] = dfTro_ai['diff'].shift(1)
dfTro_ai.to_csv('data/aiTro.csv', sep = " ")



#ACTIVITY INDEX DOMBÅS
dfDob = pd.read_csv('data/magnetometer_dob.csv', sep = " ")
dfDob = dfDob.drop_duplicates(subset=['timestamp'])

dfDob['timestamp'] = pd.to_datetime(dfDob['timestamp'])
dfDob.set_index('timestamp', inplace=True)

length_dob = len(dfDob.index)-1
for i in range(length_dob):
    if (dfDob.index[i+1]-dfDob.index[i]).seconds >= 300:
        dfDob.loc[dfDob.index[i]+dtm.timedelta(minutes=5)] = float("nan")
dfDob.sort_index(inplace=True)

dfDob_ai = dfDob.resample('H')['Horiz_dob'].agg(['max', 'min'])
dfDob_ai['diff'] = dfDob_ai['max'].sub(dfDob_ai['min'], axis = 0)
dfDob_ai['diff'] = dfDob_ai['diff'].shift(1)
dfDob_ai.to_csv('data/aiDob.csv', sep = " ")



#ACTIVITY INDEX NY ÅLESUND
dfNal = pd.read_csv('data/magnetometer_nal.csv', sep = " ")
dfNal = dfNal.drop_duplicates(subset=['timestamp'])

dfNal['timestamp'] = pd.to_datetime(dfNal['timestamp'])
dfNal.set_index('timestamp', inplace=True)

length_nal = len(dfNal.index)-1
for i in range(length_nal):
    if (dfNal.index[i+1]-dfNal.index[i]).seconds >= 300:
        dfNal.loc[dfNal.index[i]+dtm.timedelta(minutes=5)] = float("nan")
dfNal.sort_index(inplace=True)

dfNal_ai = dfNal.resample('H')['Horiz_nal'].agg(['max', 'min'])
dfNal_ai['diff'] = dfNal_ai['max'].sub(dfNal_ai['min'], axis = 0)
dfNal_ai['diff'] = dfNal_ai['diff'].shift(1)
dfNal_ai.to_csv('data/aiNal.csv', sep = " ")

#subprocess.call(['python', "../New_AI/magnetometer.py"])
subprocess.call(['python', "../New_AI/plots.py"])