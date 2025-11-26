# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 14:45:17 2021

@author: camendol
"""
import sqlite3
import pandas as pd
import spc_func_lib
import matplotlib.pyplot as plt

#Graphics
pd.options.display.float_format = '{:,.2f}'.format
#plt.style.use('dark_background')
plt.style.use('seaborn-dark')

plt.rcParams['figure.figsize'] = [15, 5]

#Config
client='Fullwell'
prior_cycle='20201231'
current_cycle='20210131'
prior_db='Fullwell_20201231'
current_db='Fullwell_20210131'
table='FUL_CUR_MEDCDP_vol'
by_var='plan_source'
vol_key=[by_var,'Yr','Mon']
volume_cols=['distinct','numrows','amt_pay','amt_req','amt_all','qty']

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

vol_screen=spc_func_lib.volume_test_pc( cur
                                        ,pri
                                        ,vol_key
                                        ,volume_cols
                                        ,cmp_beg_dt='2018-01-01'
                                        ,err_level=.04
                                        ,pri_lab=prior_cycle
                                        ,cur_lab=current_cycle)

spc_func_lib.vol_test_plot( vol_screen
                            ,list_only=True
                            ,vkey0=by_var
                            ,pri_lab=prior_cycle
                            ,cur_lab=current_cycle
                            )
spc_func_lib.vol_test_plot( vol_screen
                            ,list_only=False
                            ,vkey0=by_var
                            ,pri_lab=prior_cycle
                            ,cur_lab=current_cycle
                            )
