# -*- coding: utf-8 -*-
"""
Created on Sat May 15 09:57:44 2021

@author: camendol
"""
import sqlite3
import pandas as pd
import numpy as np
import spc_func_lib
import plotly.express as px
import plotly.io as pio
import plotly.offline as pyo
import os

def jackknife(x, func):
    """Jackknife estimate of the estimator func"""
    n = len(x)
    idx = np.arange(n)
    return np.sum(func(x[idx!=i]) for i in range(n))/float(n)

def jackknife_var(x, func):
    """Jackknife estiamte of the variance of the estimator func."""
    n = len(x)
    idx = np.arange(n)
    j_est = jackknife(x, func)
    return (n-1)/(n + 0.0) * np.sum((func(x[idx!=i]) - j_est)**2.0
                                    for i in range(n))

pio.renderers.default='browser'

#Config
client='Fullwell'
source='CDP'
client_id='FUL'
data_type='MED'
prior_cycle='20210131'
current_cycle='20210228'
by_var='plan_source'
volume_cols=['numrows','distinct','amt_pay','amt_req','amt_all','qty']

cmp_beg_dt='2018-01-01'

#### Main
prior_db=client+'_'+prior_cycle
current_db=client+'_'+current_cycle
table=client_id+'_CUR_'+data_type+source+'_vol'
vol_key=[by_var,'Yr','Mon']
pri_lab=prior_cycle
cur_lab=current_cycle

#Connect
cur_con = sqlite3.connect("\\\\apswp2286\\w\\camendol\\SPC\\data\\"+current_db+"_summ.sqlite")
pri_con = sqlite3.connect("\\\\apswp2286\\w\\camendol\\SPC\\data\\"+prior_db+"_summ.sqlite")

#Bring Data in
#Volume
cur=pd.read_sql_query('''select * from '''+table, cur_con)
pri=pd.read_sql_query('''select * from '''+table, pri_con)

cur['period']=current_cycle
pri['period']=prior_cycle

cur[by_var]=cur[by_var].str.upper()
pri[by_var]=pri[by_var].str.upper()

prep=pri.merge( cur
               ,how='outer'\
               ,left_on=vol_key\
               ,right_on=vol_key\
               ,suffixes=('_'+pri_lab, '_'+cur_lab))\
        .fillna(0)\
        .reset_index(drop=True)
                  
prep['filter_date']=pd.to_datetime(prep['Yr'].astype(str) + prep['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                         ,format='%Y%m%d'
                                         ,errors='ignore')	
    
#Get aggregates for diffs leaving out last 3 months, aka run-out
filtered=prep.groupby([vol_key[0]])\
             .apply(lambda x: x.iloc[:-3])\
             .reset_index(drop=True)
    
#Trim off early dates prior to comparison range
filtered=filtered[filtered['filter_date'] >= pd.to_datetime(cmp_beg_dt)]

#Compute deviations - direction-less differeces
dev_list=[]
for measure in volume_cols:
        # Deviation=absolute value of difference between prior and current cycle values
        # Prior is 'expected' and current is obsereved
        filtered[measure+'_dev']= abs(  filtered[measure+'_sum_'+cur_lab]\
                                       -filtered[measure+'_sum_'+pri_lab])
    
        #Allowable Dev 
        filtered[measure+'_sum_error']=filtered[measure+'_sum_'+pri_lab]*1.02
        filtered[measure+'_edev']= abs(  filtered[measure+'_sum_error']\
                                        -filtered[measure+'_sum_'+pri_lab])
                              

dev_list.append(vol_key[0]) 

filtered['Yr_Mon']=pd.to_datetime( filtered['Yr'].astype(str) + filtered['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                        ,format='%Y%m%d'
                                        ,errors='ignore')
figs=[]
for var in volume_cols:
    print(var)
    jk_n=filtered[var+'_dev'].count()-1
    mean_jk=jackknife(filtered[var+'_dev'], np.mean)
    mean_var_jk=jackknife_var(filtered[var+'_dev'], np.mean)
    se_var_jk=(mean_var_jk**1/2)*(jk_n**1/2)
    
    #var_jk=jackknife_var(filtered[var+'_dev'], np.var)
    
    n=filtered[var+'_edev'].count()
    x=filtered[var+'_edev'].mean()
    s=filtered[var+'_edev'].var()
    
    print( mean_jk,mean_jk+1.96*se_var_jk,mean_jk-1.96*se_var_jk)
    print(x,(s**1/2)*(n**1/2))
          
    filtered.plot(kind='bar',x="Yr_Mon",y=var+'_dev',title=client+'-'+source)
    filtered.plot(kind='bar',x="Yr_Mon",y=var+'_edev',title=client+'-'+source)
    
    # px.bar( filtered
    #              ,x="Yr_Mon"
    #              ,y=var+'_dev'
    #              ,barmode='group'
    #              ,height=400
    #              ,width=2000
    #              #,title=title
    #              ,template='plotly')
    