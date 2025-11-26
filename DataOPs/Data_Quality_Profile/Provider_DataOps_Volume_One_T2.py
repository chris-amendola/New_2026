# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 14:45:17 2021

@author: camendol
"""
import sqlite3
import pandas as pd
import spc_func_lib
import plotly.express as px
import plotly.io as pio
import plotly.offline as pyo
import os
import scipy.stats as stats

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

prior_db='Fullwell_20201231'
current_db='Fullwell_20210131'
table=client_id+'_CUR_'+data_type+source+'_vol'
vol_key=[by_var,'Yr','Mon']
cmp_beg_dt='2017-01-01'
significance_level=.01
error=.04

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

#Filter away the run-out months
prior_fil=pri.groupby([vol_key[0]])\
             .apply(lambda x: x.iloc[:-3])\
             .reset_index(drop=True)
#Set begin date             
prior_fil['filter_date']=pd.to_datetime( prior_fil['Yr'].astype(str) + prior_fil['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                         ,format='%Y%m%d'
                                         ,errors='ignore')	
    
#Trim off early dates prior to comparison range
prior_fil=prior_fil[prior_fil['filter_date'] >= pd.to_datetime(cmp_beg_dt)]    

#Prior values aggregates to compute upper error bound
pri_aggs=prior_fil\
              .groupby(vol_key[0])\
              .agg(['mean','count','std','sum','sem'])

for measure in volume_cols:  
    pri_aggs[(measure+'_sum'),('mu_err')]=(pri_aggs[measure+'_sum']['mean']*error)\
              +(pri_aggs[measure+'_sum']['mean'])
              
              