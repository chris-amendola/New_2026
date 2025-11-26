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
client='Tufts_II'
source='_SPAN'
client_id='THP'
data_type='ELG'
prior_cycle='20210228'
current_cycle='20210331'
by_var='map_source'

prior_db='Tufts_II_'+prior_cycle
current_db='Tufts_II_'+current_cycle
table=client_id+'_CUR_'+data_type+source+'_fqs'
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

field_res=spc_func_lib.field_fit_pc( cur
                                    ,pri
                                    ,by_var
                                    ,effect_size_threshold=.1)
#PRESENTATION????!!!!
messages=field_res['_inspect_']
messages['Report_line']='Varible: '+messages['variable']+' Value: '+messages['value']+' '+messages['freq_pri'].astype(str)+' to '+messages['freq_cur'].astype(str)
print('Details:\n',messages[['variable','value','freq_pri','freq_cur','exp_count_val']])


# Some Ad-Hoc
z=messages.loc[ ((messages['variable']=='PROV_AFFIL_MEM')
               &(messages['value']=='Missing')),['freq_pri','freq_cur','value']]
z.plot( kind='bar'
       ,x='value'
       ,title=prior_cycle+' versus '+current_cycle+' for Count Missing PROV_AFFIL_MEMBER')

y=messages.loc[ ((messages['variable']=='PROV_AFFIL_MEM')
               &(messages['value']=='Missing')),['pri_prop','cur_prop','value']]
y.plot( kind='bar'
       ,x='value'
       ,title=prior_cycle+' versus '+current_cycle+' for Proportion Missing PROV_AFFIL_MEMBER')




























# path = os.path.join("\\\\apswp2286\\w\\camendol\\SPC\\Vol_Reps\\", client+'\\'+source+"\\")

# try:
#     os.makedirs(path, exist_ok = True)
#     print("Directory '%s' created successfully" % path)
# except OSError as error:
#     print("Directory '%s' can not be created" % path)

# #Simple montoring
# if len(res)>0: 
#     review='_REVIEW'
# else:
#     review=''     

# dashboard = open("\\\\apswp2286\\w\\camendol\\SPC\\Freq_Reps\\"+client+'\\'+source+"\\"+data_type+"_Field_Report"+review+".html", 'w')
# dashboard.write("<html><head></head><body>" + "\n")

# add_js = True
# for fig in figs:
#     inner_html = pyo.plot(fig, include_plotlyjs=add_js, output_type='div')
#     dashboard.write(inner_html)
#     add_js = False

# dashboard.write("</body></html>" + "\n")
# dashboard.close()