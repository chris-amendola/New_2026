# -*- coding: utf-8 -*-
"""
Created on Fri 08.30.2020

@author: camendol
"""

# python .\dqm_vol_summary.py --database 'DVU_II_PROTO' --server 'dbswp0871'  --output_dir 'W:\DVU_Impact_Data\Molina\PD20200131\rtw\' --med_table 'stage_ii_med_1307' --phm_table 'stage_ii_phm_1307' --mem_table 'stage_ii_mem_1307' --sub_tables 'stage_ii_sub_1307'
# --phm_sum_by 'amt_pay' 'amt_req' 'amt_eqv' 'qty'
#Packages
import argparse
import logging
import pyodbc
#import numpy as np
import pandas as pd
import sqlite3


#Loacl Functions
def summ_by_yr_mon_query( date_var=''\
                         ,sum_vars=''\
                         ,by_vars=''\
                         ,data_type=''):
    
    sql_query_vars=''
    group_order_list=''
    
    for var in sum_vars:
        sql_query_vars=sql_query_vars+',sum('+var+') as '+var+'_sum'

    for by_var in by_vars:
        sql_query_vars=sql_query_vars+','+by_var+' '
        group_order_list=group_order_list+','+by_var+' '
           
    sql_query_prefix= 'select YEAR('+date_var+') as Yr, MONTH('+date_var+') as Mon, count(*) as numrows_sum, count(distinct member) as members_sum'
    
    sql_query_suffix = ' from '+data_type\
                      +' group by YEAR('+date_var+'), MONTH('+date_var+')'\
                      +group_order_list\
                      +' order by YEAR('+date_var+'), MONTH('+date_var+')'\
                      +group_order_list+';'
    
    return sql_query_prefix+sql_query_vars+sql_query_suffix

#Main
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Data Quality Monitoring Volumetrics Summary Mart Creation')

parser.add_argument( '-s'
                    ,'--server'
                    ,help='II Input Server ID'
                    ,required=True)

parser.add_argument( '-d'
                    ,'--database'
                    ,help='II Input Database'
                    ,required=True)

parser.add_argument( '-o'
                    ,'--output_dir'
                    ,help='Filepath to locate the output summary Db file'
                    ,required=True)
                    
parser.add_argument( '-m'
                    ,'--med_table'
                    ,help='Medical Services Table'
                    ,default='servicemed'
                    ,required=False)
                    
parser.add_argument( '-p'
                    ,'--phm_table'
                    ,help='Pharmacy Services Table'
                    ,default='servicerx'
                    ,required=False) 
                    
parser.add_argument( '-v'
                    ,'--by_var_list'
                    ,help='By-Group Dimensions'
                    ,default='contract'
                    ,required=False)   

parser.add_argument( '-t'
                    ,'--time_sum_col'
                    ,help='Temporal Summary Column'
                    ,default='DOS'
                    ,required=False) 

parser.add_argument( '-a'
                    ,'--med_sum_by'
                    ,help='Summarize Dimension for Medical Volumetrics'
                    ,default=['amt_pay','amt_req','amt_eqv','qty']
                    ,required=False) 

parser.add_argument( '-b'
                    ,'--phm_sum_by'
                    ,nargs='*'
                    ,help='Summarize Dimension for Pharmacy Volumetrics'
                    ,default=['amt_pay','amt_req','amt_eqv','met_qty']
                    ,required=False) 

parser.add_argument( '-e'
                    ,'--mem_table'
                    ,help='Member Table'
                    ,default='MEMBER'
                    ,required=False) 

parser.add_argument( '-f'
                    ,'--sub_table'
                    ,help='Subscriber Table'
                    ,default='SUBSCRIBER'
                    ,required=False)                     

args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)

#Source Db establishment
pyodbc.drivers()

logging.info('Establish SQL Server Connection....')
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};\
                      Server="+args.server+";\
                      database="+args.database+";\
                      Trusted_Connection=yes")

cursor = cnxn.cursor()

logging.info('Done.')

#Establish output Db file connection
#Send summary/aggregates to storage
sql_con = sqlite3.connect(args.output_dir+args.database+"_summ.sqlite")

#Volumetrics
# Basic Volumetrics- any of these could/should become set by arguments
# passed at CL
#vol_date_col='DOS'
#med_sum_by=['amt_pay','amt_req','amt_eqv','qty']
#phm_sum_by=['amt_pay','amt_req','amt_eqv','met_qty']
#med_table='servicemed'
#phm_table='servicerx'
#by_vars=['contract']
#by_var='contract'

med_table=args.med_table
phm_table=args.phm_table
by_vars=[args.by_var_list]
by_var=args.by_var_list

vol_date_col=args.time_sum_col
med_sum_by=args.med_sum_by
phm_sum_by=args.phm_sum_by
mem_table=args.mem_table
sub_table=args.sub_table


#Construct service volumetrics queries
med_vol_query=summ_by_yr_mon_query( date_var=vol_date_col\
                                ,sum_vars=med_sum_by\
                                ,data_type=med_table\
                                ,by_vars=by_vars)
#print('\n',med_vol_query,'\n')

phm_vol_query=summ_by_yr_mon_query( date_var=vol_date_col\
                                ,sum_vars=phm_sum_by\
                                ,data_type=phm_table\
                                ,by_vars=by_vars)
#print('\n',phm_vol_query,'\n')

logging.info('Service Med Volumetric summarization...')
vol_med=pd.read_sql_query(med_vol_query,cnxn)
vol_med.to_sql("servicemed",sql_con, if_exists="replace")
logging.info('Done.')

logging.info('Service Rx Volumetric summarization...')
vol_phm=pd.read_sql_query(phm_vol_query,cnxn)
vol_phm.to_sql("servicerx",sql_con, if_exists="replace")
logging.info('Done.')

#Construct member months volumetric queries
iter_table_sql='''
SELECT TOP (120)
     n = ROW_NUMBER() OVER (ORDER BY [object_id])
  INTO #iterator
  FROM sys.all_objects ORDER BY n;
'''
cursor.execute(iter_table_sql)
cnxn.commit()

mem_mmos_sql='''
SELECT DATEADD(DAY,1,(EOMONTH(DATEADD(MONTH,lag_n,EFF_DT),-1))) as mm_beg
      ,EOMONTH(DATEADD(MONTH,lag_n,EFF_DT)) as mm_end
  ,spans.*
  into #months
  from (select *
             ,DATEDIFF(month,[EFF_DT],[END_DT]) as len_months
      from '''+args.mem_table+''' ) as spans,
     (select n
           ,n-1 as lag_n
        from #iterator) as iter
where spans.len_months+1>=iter.n;
'''

cursor.execute(mem_mmos_sql)
cnxn.commit()

mem_vol_query='''
   select 'Combined' as contract       
          ,MONTH(mmos.mm_beg) as Mon
          ,YEAR(mmos.mm_beg) as Yr
          ,count(member) as mmos_sum
          ,count(distinct member) as members_sum
     from (select * from #months) as mmos
     group by  MONTH(mmos.mm_beg)
              ,YEAR(mmos.mm_beg)
     order by  YEAR(mmos.mm_beg)
             ,MONTH(mmos.mm_beg);
'''

logging.info('Member Months Volumetric summarization...')
vol_mem=pd.read_sql_query(mem_vol_query,cnxn)
vol_mem.to_sql( "member"
               ,sql_con
               ,if_exists="replace")
logging.info('Done.')

#Construct subscriber months volumetric queries
sub_smos_sql='''SELECT DATEADD(DAY,1,(EOMONTH(DATEADD(MONTH,lag_n,EFF_DT),-1))) as sm_beg
      ,EOMONTH(DATEADD(MONTH,lag_n,EFF_DT)) as sm_end
  ,spans.*
  into #sub_months
  from (select *
             ,DATEDIFF(month,[EFF_DT],[END_DT]) as len_months
      from '''+args.sub_table+''') as spans,
     (select n
           ,n-1 as lag_n
        from #iterator) as iter
where spans.len_months+1>=iter.n;
'''

cursor.execute(sub_smos_sql)
cnxn.commit()

sub_vol_query='''select contract
        ,MONTH(smos.sm_beg) as Mon
        ,YEAR(smos.sm_beg) as Yr
        ,count(subscriber_id) as smos_sum
        ,count(distinct subscriber_id) as subscribers_sum
    from(select * from #sub_months
) as smos
group by contract
        ,MONTH(smos.sm_beg)
        ,YEAR(smos.sm_beg) 
order by contract
        ,YEAR(smos.sm_beg)
        ,MONTH(smos.sm_beg);
'''
logging.info('Subscriber Months Volumetric summarization...')
vol_sub=pd.read_sql_query(sub_vol_query,cnxn)
vol_sub.to_sql("subscriber",sql_con, if_exists="replace")
logging.info('Done.')

sql_con.close()
cursor.close()
cnxn.close()
