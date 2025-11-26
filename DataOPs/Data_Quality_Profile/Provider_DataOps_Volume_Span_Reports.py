# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 14:45:17 2021

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

pio.renderers.default='browser'

#Config
client='Fullwell'
source='AET'
client_id='FUL'
data_type='ELG'
prior_cycle='20210131'
current_cycle='20210228'
by_var='contract'
volume_cols=['dmos']

#### Main
prior_db=client+'_'+prior_cycle
current_db=client+'_'+current_cycle
table=client_id+'_CUR_'+data_type+source+'_span_vol'
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
                     & (final['Yr_Mon']<= pd.to_datetime('2020-11-01'))]

final_srt=final_filtered.sort_values('period',ascending=[True],inplace=False)

err=.02
vol_screen=spc_func_lib.volume_test_pc( cur
                                        ,pri
                                        ,vol_key
                                        ,volume_cols
                                        ,cmp_beg_dt='2018-01-01'
                                        ,err_level=err
                                        ,pri_lab=prior_cycle
                                        ,cur_lab=current_cycle)

raw=vol_screen['_raw_']
raw['Yr_Mon']=pd.to_datetime( raw['Yr'].astype(str) + raw['Mon'].astype(str).str.rjust(2, '0') + '01'\
                                        ,format='%Y%m%d'
                                        ,errors='ignore')	
res=vol_screen['_results_']
stats=vol_screen['_stats_']

figs=[]

for var in volume_cols:
    
    #Scale Difs
    raw[[var+'_chg_prp']] =raw[var+'_dif']/raw[var+'_sum_'+prior_cycle]
    
    # base_com=raw[[by_var,'Yr_Mon','numrows_chg_prp']].rename(columns={"numrows_chg_prp":"value"})
    # base_com['Measure']='Numrows'
    
    # test_com=raw[[by_var,'Yr_Mon',var+'_chg_prp']].rename(columns={var+"_chg_prp":"value"})
    # test_com['Measure']=var
    
    #Arrange Scaled Dif Data for grouped bar-chart
    # scaled_com=pd.concat([base_com,test_com])
      
    p_0=res.loc[res['measure']==var,'p_0'].iloc[0]
    p_e=res.loc[res['measure']==var,'p_e'].iloc[0][0]
    act_var=res.loc[res['measure']==var,'actual_variance'].iloc[0]
    err_var=res.loc[res['measure']==var,'error_variance'].iloc[0]
    var_ratio=res.loc[res['measure']==var,'var_ratio'].iloc[0]
    crit_val=res.loc[res['measure']==var,'critical_val'].iloc[0]
    
    print(source)
    story=''
    level=0
    # then for each measure
    print('\n',var)
    if p_0<.01:
        print('Significant Change from Prior. Crit: ',.01)
        story='Significant Change from Prior values'
        level=1
    else:
        print('No significant average change from prior.')
        story='No significant average change from prior'
    print('-----')

    if p_e<.01: 
        print('Change is significantly greater than ',err,' error. CRIT: ',.01)
        story=story+' and exceeds allowed error.'
        level=level+1
    else:
        print('Change does not exceed error. Error: ',err)
        story=story+' and does not exceed allowed error.'

	# Compare ratio of chi^2 actual the theoretical error - follows an F-dsitirbution
    print('-----')
    if stats[var+'_dev']['count'][0] > 0:
        if var_ratio>crit_val:	       
           print(' Actual Variance Exceeds error allowed.')
           story=story+' Actual Variance Exceeds error allowed('+str(var_ratio)+','+str(crit_val)+').'
           level=level+1
        else:
            print(' Actual Variance within error allowed.')
    else:
        print('No source Data to compare.')
        story='No source Data to compare.'
    print('*****') 
    
    title= client+' Source: '+source+'<br>Comparison of '\
          +var+' SUM for Cycles: '+prior_cycle+' to '+current_cycle\
          +'<br>'+story
            
      
    fig = px.bar( final_srt
                 ,x="Yr_Mon"
                 ,y=var+'_sum'
                 ,color='period'
                 ,barmode='group'
                 ,height=400
                 ,width=2000
                 ,title=title
                 ,template='plotly')
    fig.update_layout(margin=dict(b=5))
    
    # fig2= px.bar( scaled_com
    #               ,x="Yr_Mon"
    #               ,y='value'
    #               ,color='Measure'
    #               ,barmode='group'
    #               ,height=250,width=2000
    #               ,title='Difference Compare'
    #               ,template='ggplot2')
    
    #fig2.update_layout(margin=dict(r=50))
    
    figs.append(fig)
    # figs.append(fig2)

path = os.path.join("\\\\apswp2286\\w\\camendol\\SPC\\Vol_Reps\\", client+"\\Review_Level_"+str(level)+'\\'+source)

try:
    os.makedirs(path, exist_ok = True)
    print("Directory '%s' created successfully" % path)
except OSError as error:
    print("Directory '%s' can not be created" % path)

#Simple monitoring
if level>5: 
    review='_REVIEW_LEV'+str(level)
else:
    review=''     

dashboard = open("\\\\apswp2286\\w\\camendol\\SPC\\Vol_Reps\\"+client+"\\Review_Level_"+str(level)+'\\'+source+'\\'+data_type+"_Volume_Report"+review+".html", 'w')
dashboard.write("<html><head></head><body>" + "\n")

add_js = True
for fig in figs:
    inner_html = pyo.plot(fig, include_plotlyjs=add_js, output_type='div')
    dashboard.write(inner_html)
    add_js = False

dashboard.write("</body></html>" + "\n")
dashboard.close()

for order,measure in enumerate(volume_cols):
    temp=final_filtered[[ 'Yr','Mon'
                        ,measure+'_sum'
                        ,'contract'
                        ,'Yr_Mon'
                        ,'period']]
    temp.loc[:,'measure']=measure
    temp.rename(columns={measure+'_sum':'value'}, inplace=True)
    if order==0:
        pivot_ready=temp
    else:
        pivot_ready=pd.concat([pivot_ready,temp])