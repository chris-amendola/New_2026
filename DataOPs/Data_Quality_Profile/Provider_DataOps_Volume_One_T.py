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
source='BRI'
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
cmp_beg_dt='2018-01-01'
significance_level=.01
error=.02

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

diffs_prep=pri.merge( cur
                     ,how='outer'\
                     ,left_on=vol_key\
                     ,right_on=vol_key\
                     ,suffixes=('_'+prior_cycle, '_'+current_cycle))\
                  .fillna(0)\
                  .reset_index(drop=True)
                  
diffs_prep['filter_date']=pd.to_datetime( diffs_prep['Yr'].astype(str) + diffs_prep['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                         ,format='%Y%m%d'
                                         ,errors='ignore')	
    
#Get aggregates for diffs leaving out last 3 months, aka run-out
diffs_filtered=diffs_prep.groupby([vol_key[0]])\
                         .apply(lambda x: x.iloc[:-3])\
                         .reset_index(drop=True)
    
#Trim off early dates prior to comparison range
diffs_filtered=diffs_filtered[diffs_filtered['filter_date'] >= pd.to_datetime(cmp_beg_dt)]

#Compute deviations - direction-less differeces
dev_list=[]
for measure in volume_cols:
    # Deviation=absolute value of difference between prior and current cycle values
    # Prior is 'expected' and current is obsereved
    diffs_filtered[measure+'_dev']= abs(diffs_filtered[measure+'_sum_'+current_cycle]\
                                   -diffs_filtered[measure+'_sum_'+prior_cycle])
    
    # Create theoretical observed values based on a simple error value 
    # 0<Error<1   
    diffs_filtered[measure+'_err']=diffs_filtered[measure+'_sum_'+prior_cycle]*error\
                                   +diffs_filtered[measure+'_sum_'+prior_cycle]
                                   
    # Deviation of expected values from theoretical observered.                            
    diffs_filtered[measure+'_ul']=abs( diffs_filtered[measure+'_err']\
                                      -diffs_filtered[measure+'_sum_'+prior_cycle])  

    # X^2=SUM((O-E)^2)/E)
    # Chi for the actual observed values
    diffs_filtered[measure+'_chi_act']=(diffs_filtered[measure+'_dev']**2)/diffs_filtered[measure+'_sum_'+prior_cycle]   
    # Chi for theortical 'error' values
    diffs_filtered[measure+'_chi_err']=(diffs_filtered[measure+'_ul']**2)/diffs_filtered[measure+'_sum_'+prior_cycle]                       
                                   
    
    dev_list.append((measure+'_dev'))
    dev_list.append((measure+'_ul'))
    dev_list.append((measure+'_chi_act'))   
    dev_list.append((measure+'_chi_err'))                         

dev_list.append(by_var) 

x=diffs_filtered.filter(regex='numrows',axis=1)

#Aggregates of 'stable period' variables - by Year-Mon
diffs_agg=diffs_filtered[dev_list]\
          .groupby(vol_key[0])\
          .agg(['mean','count','std','sum'])
   
samp=diffs_agg.filter(regex='qty',axis=1)
          
for measure in volume_cols:
    print('\n',measure)
    print('-----')            
    print(str(diffs_agg[measure+'_dev']['mean'][0]),'-',str(diffs_agg[measure+'_ul']['mean'][0]))  
    print('-----') 
    print('Greater than 0')
    print(stats.ttest_1samp( diffs_filtered[measure+'_dev']
                            ,0
                            ,alternative='greater'))
    print('-----')
    print('Greater than ',error,' error')
    print(stats.ttest_1samp( diffs_filtered[measure+'_dev']
                            ,diffs_agg[(measure+'_ul'),('mean')]
                            ,alternative='greater')) 
    # Compare ratio of chi^2 actual the theoretical error - follows an F-dsitirbution
    print( 'ACT: ',diffs_agg[measure+'_chi_act']['sum'][0]/34,' ERR: ',diffs_agg[measure+'_chi_err']['sum'][0]/34) 
    print( (diffs_agg[measure+'_chi_act']['sum'][0]/34)/(diffs_agg[measure+'_chi_err']['sum'][0]/34))
    print(stats.f.ppf(1-significance_level, 34, 34))
    print('*****')   
    