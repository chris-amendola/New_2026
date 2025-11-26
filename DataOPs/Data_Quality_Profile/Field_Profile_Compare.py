# -*- coding: utf-8 -*-
"""
Created on Sun Mar 14 18:48:23 2021

@author: camendol
"""
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as st
import seaborn
import spc_func_lib
from pandas.plotting import table

#Config
prior_db='II_INPUT_LEHIGH72_PD20210131_INC20201031'
current_db='II_INPUT_LEHIGH72_PD20210228_INC20201130'

#Config
out_dir='W:\camendol\Field_Level_Change_Detection\data'
#Volumetrics
# Basics Volumetrics
med_volume_cols=['qty','cost','met_qty','charged','allowed']
#['qty','cost','met_qty','charged','allowed','copay','COINSURANCE','DEDUCTIBLE']
mem_vol_cols=['mmos','members']
serve_table='service'

by_var='contract'
by_vars=[by_var]

vol_key=[by_var,'Yr','Mon']

#Graphics
pd.options.display.float_format = '{:,.2f}'.format
plt.style.use('seaborn-colorblind')
plt.rcParams['figure.figsize'] = [15, 5]

#Connect
cur_con = sqlite3.connect("\\\\apswp2286\\w\\camendol\\SPC\\data\\"+current_db+"_summ.sqlite")
pri_con = sqlite3.connect("\\\\apswp2286\\w\\camendol\\SPC\\data\\"+prior_db+"_summ.sqlite")

#Bring Data in
#Field Freqs
cur_med_fqs=pd.read_sql_query('''select * from med_fields''', cur_con)
pri_med_fqs=pd.read_sql_query('''select * from med_fields''', pri_con)

cur_phm_fqs=pd.read_sql_query('''select * from phm_fields''', cur_con)
pri_phm_fqs=pd.read_sql_query('''select * from phm_fields''', pri_con)

cur_med_fqs[by_var]=cur_med_fqs[by_var].str.upper()
pri_med_fqs[by_var]=pri_med_fqs[by_var].str.upper()

cur_phm_fqs[by_var]=cur_phm_fqs[by_var].str.upper()
pri_phm_fqs[by_var]=pri_phm_fqs[by_var].str.upper()

inspect=[]
inspect=spc_func_lib.field_fit_pc ( cur_med_fqs
                                   ,pri_med_fqs
                                   ,by_var
                                   ,effect_size_threshold=.1)

inspect.sort_values('variable')

#### Missing Values
plt.rcParams['figure.figsize'] = [5, 15]
for source in inspect[by_var].unique():
    temp=inspect.loc[ ((inspect[by_var]==source)
                      &(inspect['value']=='Missing'))                     
                     ,('pri_prop','cur_prop','variable')]
    if len(temp)>0:
        z=temp
        title=source
        fig, ax = plt.subplots(1, 1)
        table( ax
              ,np.round(temp, 2)
              ,loc='right'
              ,colWidths=[0.1, 0.1, 0.1])
        temp.plot( ax=ax
                  ,kind='barh'\
                  ,title=title\
                  ,x='variable')

plt.rcParams['figure.figsize'] = [15, 5]
for source in inspect[by_var].unique():
    #print(source)
    for var in inspect.loc[inspect[by_var]==source,'variable'].unique():
        #print('  ',var)
        #Consider two different display modes - when prop_diff less than .01 show raw values?
        temp=inspect.loc[ ((inspect[by_var]==source)
                          &(inspect['variable']==var)
                          &(inspect['value']!='Missing'))                   
                         ,('value','pri_prop','cur_prop')]
        title=source+'-'+var
        if len(temp)>0:
            fig, ax = plt.subplots(1, 1)
            table(ax, np.round(temp, 2),loc='right', colWidths=[0.1, 0.1, 0.1])
            temp.plot( ax=ax
                      ,kind='bar'                      
                      ,title=title                      
                      ,x='value')

inspect=[]
inspect=spc_func_lib.field_fit_pc ( cur_phm_fqs
                                   ,pri_phm_fqs
                                   ,by_var
                                   ,effect_size_threshold=.1)

inspect.sort_values('variable')

#### Missing Values
plt.rcParams['figure.figsize'] = [5, 15]
for source in inspect[by_var].unique():
    temp=inspect.loc[ ((inspect[by_var]==source)
                      &(inspect['value']=='Missing'))                     
                     ,('pri_prop','cur_prop','variable')]
    if len(temp)>0:
        z=temp
        title=source
        fig, ax = plt.subplots(1, 1)
        table( ax
              ,np.round(temp, 2)
              ,loc='right'
              ,colWidths=[0.1, 0.1, 0.1])
        temp.plot( ax=ax
                  ,kind='barh'\
                  ,title=title\
                  ,x='variable')

plt.rcParams['figure.figsize'] = [15, 5]
for source in inspect[by_var].unique():
    #print(source)
    for var in inspect.loc[inspect[by_var]==source,'variable'].unique():
        #print('  ',var)
        #Consider two different display modes - when prop_diff less than .01 show raw values?
        temp=inspect.loc[ ((inspect[by_var]==source)
                          &(inspect['variable']==var)
                          &(inspect['value']!='Missing'))                   
                         ,('value','pri_prop','cur_prop')]
        title=source+'-'+var
        if len(temp)>0:
            fig, ax = plt.subplots(1, 1)
            table(ax, np.round(temp, 2),loc='right', colWidths=[0.1, 0.1, 0.1])
            temp.plot( ax=ax
                      ,kind='bar'                      
                      ,title=title                      
                      ,x='value')

