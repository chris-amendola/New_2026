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

#Config
prior_db='II_THP_INPUT73_PD20210131_INC20201031'
current_db='II_THP_INPUT73_PD20210228_INC20201130'

#Config
out_dir='W:\camendol\Field_Level_Change_Detection\data'
#Volumetrics
#IP
# Basics Volumetrics
# med_volume_cols=['qty','paid','met_qty','charged','allowed']
# mem_vol_cols=['mmos','members']

serve_table='service'

by_var='MAP_SRCE_N'
by_vars=['MAP_SRCE_N']

vol_key=[by_var,'Yr','Mon']

#II
med_volume_cols=['amt_pay','amt_req','amt_eqv','qty','members','numrows']
phm_volume_cols=['amt_pay','amt_req','amt_eqv','met_qty','members','numrows']
mem_vol_cols=['mmos','members']
sub_vol_cols=['smos','subscribers']

#Graphics
pd.options.display.float_format = '{:,.2f}'.format
plt.style.use('seaborn-colorblind')
plt.rcParams['figure.figsize'] = [15, 5]

#Connect
cur_con = sqlite3.connect("\\\\apswp2286\\w\\camendol\\SPC\\data\\"+current_db+"_summ.sqlite")
pri_con = sqlite3.connect("\\\\apswp2286\\w\\camendol\\SPC\\data\\"+prior_db+"_summ.sqlite")

#Bring Data in
#Volume
cur_med=pd.read_sql_query('''select * from servicemed''', cur_con)
pri_med=pd.read_sql_query('''select * from servicemed''', pri_con)
cur_phm=pd.read_sql_query('''select * from servicerx''', cur_con)
pri_phm=pd.read_sql_query('''select * from servicerx''', pri_con)
cur_mem=pd.read_sql_query('''select * from member''',cur_con)
pri_mem=pd.read_sql_query('''select * from member''',pri_con)
cur_sub=pd.read_sql_query('''select * from subscriber''',cur_con)
pri_sub=pd.read_sql_query('''select * from subscriber''',pri_con)

cur_med['period']='Current'
pri_med['period']='Prior'
cur_phm['period']='Current'
pri_phm['period']='Prior'

cur_med[by_var]=cur_med[by_var].str.upper()
pri_med[by_var]=pri_med[by_var].str.upper()
cur_phm[by_var]=cur_phm[by_var].str.upper()
pri_phm[by_var]=pri_phm[by_var].str.upper()

cur_mem[by_var]='*'
pri_mem[by_var]='*'
cur_sub[by_var]='*'
pri_sub[by_var]='*'

cur_mem['period']='Current'
pri_mem['period']='Prior'

cur_sub['period']='Current'
pri_sub['period']='Prior'

med_periods=[cur_med,pri_med]
med_final = pd.concat(med_periods)
phm_periods=[cur_phm,pri_phm]
phm_final = pd.concat(phm_periods)
mem_periods=[cur_mem,pri_mem]
mem_final = pd.concat(mem_periods)
sub_periods=[cur_sub,pri_sub]
sub_final = pd.concat(sub_periods)

med_vol_screen=spc_func_lib.volume_test_pc( cur_med
                                           ,pri_med
                                           ,vol_key
                                           ,med_volume_cols
                                           ,cmp_beg_dt='2017-12-01'
                                           ,err_level=.02                                           )

phm_vol_screen=spc_func_lib.volume_test_pc( cur_phm
                                            ,pri_phm
                                            ,vol_key
                                            ,phm_volume_cols
                                            ,cmp_beg_dt='2017-12-01'
                                            ,err_level=.02)

mem_vol_screen=spc_func_lib.volume_test_pc( cur_mem
                                           ,pri_mem
                                           ,[by_var,'Yr','Mon']
                                           ,mem_vol_cols
                                           ,cmp_beg_dt='2017-12-01'
                                           ,err_level=.02
                                           )
sub_vol_screen=spc_func_lib.volume_test_pc( cur_sub
                                            ,pri_sub
                                            ,vol_key
                                            ,sub_vol_cols
                                            ,cmp_beg_dt='2017-12-01'
                                            ,err_level=.02)

spc_func_lib.vol_test_plot( med_vol_screen
                           ,list_only=False
                           ,vkey0=by_var
                            )

spc_func_lib.vol_test_plot( phm_vol_screen
                            ,list_only=True)

spc_func_lib.vol_test_plot( mem_vol_screen
                           ,list_only=True)

spc_func_lib.vol_test_plot( sub_vol_screen
                            ,list_only=True)


