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

combine_list=[cur,pri]
final = pd.concat(combine_list)

final['Yr_Mon']=pd.to_datetime( final['Yr'].astype(str) + final['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                        ,format='%Y%m%d'
                                        ,errors='ignore')	
    
final_filtered=final[ (final['Yr_Mon'] >= pd.to_datetime('2018-01-01'))
                     & (final['Yr_Mon']<= pd.to_datetime('2020-10-01'))]

final_srt=final_filtered.sort_values('period',ascending=[True],inplace=False)


for order,measure in enumerate(volume_cols):
    print(order)
    temp=final_srt[['Yr','Mon',measure+'_sum','plan_source','period','Yr_Mon']]
    temp.loc[:,['measure']]=measure
    temp.rename(columns={measure+'_sum':'value'}, inplace=True)
    if order==0:
        pivot_ready=temp
    else:
        pivot_ready=pd.concat([pivot_ready,temp])