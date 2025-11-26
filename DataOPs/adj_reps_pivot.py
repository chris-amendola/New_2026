# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 07:29:21 2021

@author: camendol
"""
#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import datetime as dt
from pivottablejs import pivot_ui
#import pivottablejs
import matplotlib.pyplot as plt

import sqlite3


table='servicemed'
volume_cols=['numrows','members','amt_pay','amt_req','amt_eqv','qty']

#Connect
con = sqlite3.connect("H:\\SPC-QA\\twv_base_20210228_summ.sqlite")

#Bring Data in
data=pd.read_sql_query('''select * from '''+table, con)
data['Yr_Mon']=pd.to_datetime( data['Yr'].astype(str) + data['Mon']\
                                         .astype(str)\
                                         .str.rjust(2, '0') + '01'\
                                        ,format='%Y%m%d'
                                        ,errors='ignore')	

data_srt=data.sort_values('Yr_Mon',ascending=[True],inplace=False)

for order,measure in enumerate(volume_cols):
    temp=data_srt[[ 'Yr','Mon'
                   ,'adj_stat1'
                   ,'adj_stat2'
                   ,'adj_stat3'
                   ,'keep_std_med'
                   ,'adj_flag'
                   ,measure+'_sum'
                   ,'contract'
                   ,'Yr_Mon']]
    temp.loc[:,'measure']=measure
    temp.rename(columns={measure+'_sum':'value'}, inplace=True)
    if order==0:
        pivot_ready=temp
    else:
        pivot_ready=pd.concat([pivot_ready,temp])
  
pivot_ui( pivot_ready
         ,rows=['period','measure']
         ,cols=['Yr_Mon']
         ,rendererName='Line Chart'
         ,vals=['value']
         ,aggregatorName='Sum'
         ,outfile_path="C:\\Users\\camendol\\Documents\\Adj_Test_Vol.html")    


