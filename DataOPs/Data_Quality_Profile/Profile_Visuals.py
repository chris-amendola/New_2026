# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 15:11:35 2021

@author: camendol
"""
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as st
import seaborn
import spc_func_lib

database='Fullwell_20210131' 
output_dir='W:\\camendol\\SPC\\data\\'
by_var='plan_source'
table='FUL_CUR_MEDAET'
has_vol=False
has_fqs=True

sql_con = sqlite3.connect(output_dir+database+"_summ.sqlite")

#Bring Data in
#Field Freqs
print('Load Column Freqs')
fqs=pd.read_sql_query('''select * from '''+table+'''_fqs''', sql_con)
fqs['contract']=fqs[by_var].str.upper()

#Bring Data in
#Volume
print('Load Volumetrics')
vol=pd.read_sql_query('''select * from '''+table+'''_vol''', sql_con)

vol['contract']=vol[by_var].str.upper()
vol['Yr_Mon']=vol['Mon'].map(str)+'-'+vol['Yr'].map(str)
sql_con.close()

print('Compute Freq Proportions')
#Med - total rows per client
row_tots=vol[[by_var,'numrows_sum']].groupby(by_var).sum()

#Merge Row Totals onto frequencies
fqs_tot=fqs.merge( row_tots
                  ,how='right'\
                  ,left_on=by_var
                  ,right_on=by_var\
                  ,suffixes=('_vol', '_tot'))\
           .fillna(0)\
           .reset_index(drop=True)
                  
#Get proportion of total for values
fqs_tot['prop']=fqs_tot['freq']/fqs_tot['numrows_sum']

plt.style.use('seaborn-colorblind')
plt.rcParams['figure.figsize'] = [15, 5]
sources=vol[by_var].unique()

print('Volume Plots')
for src in sources:
    title='Client: H984216  Source: '+src
    vol.loc[vol[by_var]==src]\
           .plot( y=['numrows_sum','distinct_sum','amt_pay_sum','amt_req_sum','amt_all_sum','qty_sum']
                 ,x='Yr_Mon'
                 ,subplots=True
                 ,layout=(1,6)
                 ,title=title
                 ,kind='bar')

# Volume Member Months
# mem_vol.plot( y=['mmos_sum','members_sum']
#                  ,x='Yr_Mon'
#                  ,subplots=True
#                  ,layout=(1,6)
#                  ,title=title
#                  ,kind='bar')
           
# Missing Values report
print('Missing Value Plots')
plt.rcParams['figure.figsize'] = [5, 15]
for src in sources:
    title='Client: H984216  Source: '+src
    med_fqs_tot.loc[( (med_fqs_tot[by_var]==src)
                 &(med_fqs_tot['value']=='Missing'))]\
    .sort_values('freq')\
    .plot( kind='barh'
          ,y=['prop']
          ,x='variable'
          ,title=title)   
    
#Top 20 Values
z=med_fqs_tot[[by_var,'variable','value','freq','prop']]\
            .sort_values( [by_var,'variable','freq']
                          ,ascending=False)\
           .groupby([by_var,'variable'])\
           .head(20) 
      
for src in sources:
    if src=='*':
        src='ovr'
    vars=z.loc[z[by_var]==src,'variable'].unique()
    for var in vars:
        z.loc[ ((z[by_var]==src)&(z['variable']==var))
              ,['variable','value','freq','prop']]\
            .sort_values( ['freq'],ascending=False)\
            .to_html( 'W:\\camendol\\SPC\\'+src+'_'+var+'.html',index=False)    