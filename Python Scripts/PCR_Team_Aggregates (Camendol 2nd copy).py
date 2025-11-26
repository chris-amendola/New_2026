# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import datetime as dt
from pivottablejs import pivot_ui
#import pivottablejs
import matplotlib.pyplot as plt

path='C:\\Users\\camendol\\Desktop\\'
file='PCRs.xlsx'

file_path=path+file

#Open connection to EXCEL
xl_file = pd.ExcelFile(file_path)
xl_file.sheet_names 

#Read a tab
pcr_data=xl_file.parse('PCRs')

#Apply Date typing
pcr_data['Created']=pd.to_datetime(pcr_data['Creation Date'].str[0:20])
pcr_data['Created_yr_mon'] = pd.to_datetime(pcr_data['Created'].dt.date)\
                               .map(lambda dt: dt.replace(day=1))
pcr_data['Created_yr'] = pd.to_datetime(pcr_data['Created'].dt.date)\
                           .map(lambda dt: dt.replace(day=1,month=1))
pcr_data['Updated']=pd.to_datetime(pcr_data['Last Update Date'].str[0:20])
pcr_data['Duration']=pcr_data['Updated'] - pcr_data['Created']
pcr_data['Duration']=pcr_data['Duration']/np.timedelta64(1,'D')

pcr_data['QA Expected']=pd.to_datetime(pcr_data['Expected Date to QA'].str[0:20])
pcr_data['QA Delivered']=pd.to_datetime(pcr_data['Actual Date to QA'].str[0:20])
pcr_data['PCR Approved']=pd.to_datetime(pcr_data['New PCR Approval Date'].str[0:20]) 

pcr_data['Update_Elapsed']=(dt.datetime.today() - pcr_data['Updated']).dt.days 
#Maybe bin off day buckets
bin_30s = [ -1,15,30,45, 60, 75 ,90, 120, 150, 180, 210, 240, 270, 300
           ,330, 360, np.inf]
bin_labels = [ '15','30', '45','60','75' ,'90', '120'
              ,'150','180','210','240','270','300','330','360','361+']
pcr_data['Updated_Seg']=pd.cut(pcr_data['Update_Elapsed'],bin_30s,labels=bin_labels)
pcr_data['Duration_Seg']=pd.cut(pcr_data['Duration'],bin_30s,labels=bin_labels)
#pcr_data.loc[pcr_data['Updated_Seg'].isna()]
#Week Bins
week_bin = [ -1,7,14,21,28,35,42,49,56,63,70,77,84,91,98,105,112,119,126,133
            ,140,147,154,161,168,175,182,np.inf]
week_labels = [ '1','2','3','4','5','6','7','8','9','10','11','12','13','14'
               ,'15','16','17','18','19','20','21','22','23','24','25','26','27+']

pcr_data['Updated_WKs']=pd.cut(pcr_data['Update_Elapsed'],week_bin,labels=week_labels)

current_team=[ 'Lois Nordstrom'              
              ,'Sue Szubzda'              
              ,'Tricia Huebner'              
              ,'Padmaja Gandhi'              
              ,'Pragada Kumar'              
              ,'Hiranmayee Vemulapalli'              
              ,'Christopher Amendola'              
              ,'Phanikumar Jetti'              
              ,''              
              ,' ']

#Created:Year-to-date
created_YTD=pcr_data.loc[ ((pcr_data['Owner'].isin(current_team) | pcr_data['Owner'].isnull())
                               & (pcr_data['Created']>'2021-01-01'))\
                               ,( 'Formatted ID'\
                                 ,'Created'\
                                 ,'Updated'\
                                 ,'Created_yr_mon'\
                                 ,'Created_yr'\
                                 ,'Updated_Seg'\
                                 ,'Updated_WKs'\
                                 ,'Duration_Seg'\
                                 ,'Duration'\
                                 ,'Name'\
                                 ,'Impact Kanban State'\
                                 ,'Owner')]\
        .sort_values(by='Created')
len(created_YTD)


#Created:Historical
created_hist=pcr_data.loc[ ((pcr_data['Owner'].isin(current_team) | pcr_data['Owner'].isnull())
                               & (pcr_data['Created']>'2019-01-01'))\
                               ,( 'Formatted ID'\
                                 ,'Created'\
                                 ,'Updated'\
                                 ,'Created_yr_mon'\
                                 ,'Created_yr'\
                                 ,'Updated_Seg'\
                                 ,'Updated_WKs'\
                                 ,'Duration_Seg'\
                                 ,'Duration'\
                                 ,'Name'\
                                 ,'Impact Kanban State'\
                                 ,'Owner')]\
        .sort_values(by='Created')
len(created_hist)

completed_21=pcr_data.loc[( (pcr_data['Owner'].isin(current_team)) & (pcr_data['Impact Kanban State']=='Released')                             | (pcr_data['Impact Kanban State']=='UAT'))                             & (pcr_data['Created_yr']=='2021-01-01')                            
                          ,( 'Formatted ID'\
                                 ,'Created'\
                                 ,'Updated'\
                                 ,'Created_yr_mon'\
                                 ,'Duration_Seg'\
                                 ,'Duration'\
                                 ,'Name'\
                                 ,'Impact Kanban State'\
                                 ,'Owner')]
len(completed_21)

active=pcr_data.loc[(( ( pcr_data['Owner'].isin(current_team) | pcr_data['Owner'].isnull() ) )& ((pcr_data['Impact Kanban State']=='Dev')                           | (pcr_data['Impact Kanban State']=='Approval')                          | (pcr_data['Impact Kanban State']=='Requests')
                          | (pcr_data['Impact Kanban State']=='Code Complete')
                          | (pcr_data['Impact Kanban State']=='Pending Approval')))\
                          ,( 'Formatted ID'\
                                 ,'Created'\
                                 ,'Updated'\
                                 ,'Created_yr_mon'\
                                 ,'Created_yr'\
                                 ,'Updated_Seg'\
                                 ,'Updated_WKs'\
                                 ,'Duration_Seg'\
                                 ,'Duration'\
                                 ,'Name'\
                                 ,'Impact Kanban State'\
                                 ,'Owner'
                                 ,'Blocked')]

len(active.loc[active['Blocked']==True])

print('Active Requests 2021: '+str(len(active)))
print('\tCurrently Blocked: '+str(len(active.loc[active['Blocked']==True])))

# ### Active Requests 2021: 
# ### January 3rd, 2021  -  34
# ### February 3rd, 2021 - 51   !!!!!!!!
# ### February 10th 2021 - 47 !!!!!
# ### February 26th - 40 !!!!!
# ### March 3rd - 39 !!!!!
# ### -----------------------------
# ### March 20th:
# ### Active Requests 2021: 36
# ### Currently Blocked: 12
# ### -----------------------------
# ### April 6th:
# ### Active Requests 2021: 32
# ### Currently Blocked: 10
# ### -----------------------------
# ### April 22nd:
# ### Active Requests: 31
# ### Currently Blocked: 11

plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = [15, 5]

pivot_ui( created_hist
         ,rows=['Created_yr']
         ,cols=['Created_yr_mon']
         ,rendererName='Bar Chart'
         ,outfile_path="C:\\Users\\camendol\\Documents\\PCRs\\PCRS_Created.html",title='TEST')

pivot_ui( active
         ,cols=['Updated_WKs']
         ,rendererName='Bar Chart'
         ,outfile_path="C:\\Users\\camendol\\Documents\\PCRs\\PCRS_Updated.html")

pivot_ui( completed_21
         ,cols=['Duration_Seg']
         ,rendererName='Bar Chart'
         ,outfile_path="C:\\Users\\camendol\\Documents\\PCRs\\PCRS_Completed.html")

#Create Excel sheets for each person
for whom in active['Owner'].unique():
    
    excel_book='C:\\Users\\camendol\\Documents\\PCRs\\'+whom+'.xlsx'
    current=active.loc[(active['Owner']==whom)                       ,(  'Name'                          ,'Formatted ID'                          ,'Owner'                          ,'Updated_WKs'                          ,'Created_yr'                          ,'Duration'                          ,'Impact Kanban State')]
    
    comps=completed_21.loc[(completed_21['Owner']==whom),('Name','Formatted ID','Owner','Created_yr_mon','Duration','Impact Kanban State')]

    print(excel_book)
    with pd.ExcelWriter(excel_book) as writer:
        pd.DataFrame(current)        .to_excel(writer, sheet_name='Active')
        pd.DataFrame(comps)        .to_excel(writer, sheet_name='Completed')


csv_file='C:\\Users\\camendol\\Documents\\PCRs\\Centura_PCRS.csv'

states=['Dev','Pending Approval','Requests']
pcr_data.loc[  ( pcr_data['Name'].str.contains('Centura') 
                & pcr_data['Impact Kanban State'].isin(states))
             ,['Formatted ID','Name','Owner'] ].to_csv(csv_file)

csv_file='C:\\Users\\camendol\\Documents\\PCRs\\Centura_PCRS_total.csv'

states=['Dev','Pending Approval','Requests']
pcr_data.loc[  (( pcr_data['Name'].str.contains('Centura') 
                | pcr_data['Name'].str.contains('Fullwell')) 
               & (pcr_data['Created_yr']>'2019-12-31') )
             ,['Formatted ID','Name','Owner','Created_yr_mon'] ]\
        .sort_values(by='Created_yr_mon')\
        .to_csv(csv_file)
        
        
#AD HOC
z=created_YTD[['Name']] 
new=z["Name"].str.split(" ", n = 1, expand = True)
z["Source"]=new[0]

y=z.groupby("Source").count()
y['Label']='Misc'
x=y[['Label','Name']].groupby('Label').sum().sort_values('Name',ascending=False)

q=y[['Label','Name']].groupby('Label').sum()

print(y)
#                     Name      Label
# Source                             
# (Copy                  1       Misc
# Americhoice/UHC        2        UHC
# BCBSMA:                1     BCBSMA
# BCBSNC:                1     BSBSNC
# Benevera:              1   BENEVERA
# CDPHP:                 1      CDPHP
# CHI_WA:                1     CHI_WA
# Canopy                 3     Canopy
# Canopy:                2     Canopy
# Centura(Fullwell):     1   Fullwell
# Centura/Fullwell       1   Fullwell
# Common                 2     Common
# Common:                9     Common
# Crosswalk              1       Misc
# DCM                    1        DCM
# Dean                   1       Dean
# H642                   3       Vera
# IDB:                   2        IDB
# Ipro                   1       IPRO
# Keck                   1       Keck
# Keck/H743:             1       Keck
# Keck_USC:              1       Keck
# Lehigh_OPA:            1     Lehigh
# MVP                    1        MVP
# NYU                    1        NYU
# NYU-Add                1        NYU
# NYU:                   1        NYU
# NYU\FID                1        NYU
# OhioHealth:            1       Ohio
# Palmetto:              1   Palmetto
# Preferred              1   Pref_One
# ProHealth:             1  ProHealth
# Product                1       Misc
# Regression             1       Misc
# TWV                    1        TWV
# Temple:                1     Temple
# Tufts_II:              2      Tufts
# UC_Davis_OPA:          1   UC_Davis
# UHC                    3        UHC
# USB                    2        USB
# Vera                   2       Vera
# Vera:                  1       Vera
# v12                    1       Misc