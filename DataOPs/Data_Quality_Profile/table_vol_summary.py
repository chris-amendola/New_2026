# -*- coding: utf-8 -*-
"""
Created on Fri 08.30.2020

@author: camendol
"""

#Packages
import argparse
import logging
import pyodbc
import pandas as pd
import sqlite3

#Loacl Functionso documentation available 
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
           
    sql_query_prefix= '''select YEAR('''+date_var+''') as Yr
                               ,MONTH('''+date_var+''') as Mon
                               ,count(*) as numrows_sum
                               , count(distinct member) as members_sum'''
    
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
                    
parser.add_argument( '-t'
                    ,'--table'
                    ,help='Data Table'
                    ,default='servicemed'
                    ,required=False)
                    
parser.add_argument( '-v'
                    ,'--by_var_list'
                    ,help='By-Group Dimensions'
                    ,default='contract'
                    ,required=False)   

parser.add_argument( '-c'
                    ,'--time_sum_col'
                    ,help='Temporal Summary Column'
                    ,default='DOS'
                    ,required=False) 

parser.add_argument( '-b'
                    ,'--sum_by'
                    ,help='Summarize Dimension for Volumetrics'
                    ,default=['amt_pay','amt_req','amt_eqv','qty']
                    ,required=False) 

parser.add_argument( '-e'
                    ,'--Span Table'
                    ,help='Member or Subscriber Table Flag'
                    ,default=False
                    ,required=False) 

parser.add_argument( '-q'
                    ,'--span_beg_col'
                    ,help='Span Begin Column'
                    ,default='EFF_DT'
                    ,required=False) 

parser.add_argument( '-z'
                    ,'--distinct_unit_col'
                    ,help='Span column Member or other distinct Identifer'
                    ,default='END_DT'
                    ,required=False) 

parser.add_argument
                   
args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)
logging.info("--Table: "+args.table)

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
table=args.table
by_vars=[args.by_var_list]
by_var=args.by_var_list

vol_date_col=args.time_sum_col
sum_by=args.sum_by

if args.span_table==True:
    
    #Construct member months volumetric queries
    iter_table_sql='''
        SELECT TOP (120)
          n = ROW_NUMBER() OVER (ORDER BY [object_id])
         INTO #iterator
       FROM sys.all_objects ORDER BY n;
    '''
    cursor.execute(iter_table_sql)
    cnxn.commit()

    mmos_sql='''
      SELECT DATEADD(DAY,1,(EOMONTH(DATEADD(MONTH,lag_n,'''+args.span_beg_col+'''),-1))) as mm_beg
            ,EOMONTH(DATEADD(MONTH,lag_n,'''+args.span_beg_col+''')) as mm_end
            ,spans.*
        into #months
        from (select *
                    ,DATEDIFF(month,['''+args.span_beg_col+'''],['''+args.span_end_col+''']) as len_months
               from '''+args.mem_table+''' ) as spans,
        (select n
               ,n-1 as lag_n
           from #iterator) as iter
       where spans.len_months+1>=iter.n;
    '''
    
    cursor.execute(mmos_sql)
    cnxn.commit()

    vol_query='''
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
    
else:
    #Construct service volumetrics queries
    vol_query=summ_by_yr_mon_query( date_var=vol_date_col\
                                   ,sum_vars=sum_by\
                                   ,data_type=table\
                                   ,by_vars=by_vars)

logging.info('Table Volumetric summarization...')

vol=pd.read_sql_query(vol_query,cnxn)
vol.to_sql( table+"_vol"
           ,sql_con
           ,if_exists="replace")
logging.info('Done.')
    
logging.info('Span Months Volumetric summarization...')
vol=pd.read_sql_query(vol_query,cnxn)
vol.to_sql( table+"_vol"
           ,sql_con
           ,if_exists="replace")

logging.info('Done.')

sql_con.close()
cursor.close()
cnxn.close()
