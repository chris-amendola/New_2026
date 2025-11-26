# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 09:40:05 2020

@author: camendol
"""
# python .\dqm_field_summary.py --database 'DVU_II_Proto'  --server 'dbswp0871' --output_dir 'W:\DVU_Impact_Data\Molina\PD20200131\rtw' --med_table 'stage_ii_med_1307'--phm_table 'stage_ii_phm_1307'--by_var_list 'contract'

#Packages
import argparse
import logging
import pyodbc
#import numpy as np
import pandas as pd
import sqlite3

#Internal Functions
def freq_summ(table,column_list,by_var,skip_list=[]):
    sql=''
    for item_num,column in enumerate(column_list):
        
        if column not in skip_list:
            if item_num>0:
                sql=sql+"   UNION ALL "
            
            sql=sql+"\nSELECT "+by_var\
               +"     ,'"+column+"' as variable"\
               +"       ,'Missing' as value"\
               +"       ,count(*) as freq "\
               +" FROM "+table\
               +" WHERE ["+column+"] IS NULL "\
               +" GROUP BY "+by_var\
               +" \nUNION ALL\n "\
               +" SELECT "+by_var\
               +"      ,'"+column+"' as variable"\
               +"       ,["+column+"] as value"\
               +"      ,count("+column+") as freq"\
               +"  FROM "+table\
               +"  WHERE "+column+" IS NOT NULL "\
               +"  GROUP BY "+column+","+by_var
 
    return sql+";"

def info_schema_cols(table,sql_con):
    
    table_meta= pd.read_sql_query(
    '''SELECT      COLUMN_NAME AS 'ColumnName'
                  ,TABLE_NAME AS  'TableName'
                 ,DATA_TYPE AS 'TYPE'
        FROM        INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME='''+"'"+table+"'"+'''
              AND DATA_TYPE IN ('VARCHAR','CHAR')
        ORDER BY    TableName
                   ,ColumnName;
    ''', sql_con)
    
    return (table_meta['ColumnName'])

#Main
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Data Quality Monitoring Field Summary Mart Creation')

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

args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)
logging.info("--Output Dir:"+args.output_dir)

#Source Db establishment
#SQL Server
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

med_table=args.med_table
phm_table=args.phm_table
by_vars=[args.by_var_list]
by_var=args.by_var_list

#Frequency Analysis
med_col_list=info_schema_cols(med_table,cnxn)
phm_col_list=info_schema_cols(phm_table,cnxn)

#Frequency analysis query construction and execution.
logging.info('Service Med Field summarization...')
med_freq_query=freq_summ( med_table\
                         ,med_col_list\
                         ,by_var\
                         ,skip_list=['UNIQ_REC_ID','CUST_MED_PK_ID'])
#print('\n',med_freq_query,'\n')
med_var=pd.read_sql_query(med_freq_query,cnxn)
logging.info('Done.')

logging.info('Service PHM Field summarization...')
phm_freq_query=freq_summ( phm_table\
                         ,phm_col_list\
                         ,by_var\
                         ,skip_list=['UNIQ_REC_ID','CUST_PHM_PK_ID'])
#print('\n',phm_freq_query,'\n')
phm_var=pd.read_sql_query(phm_freq_query,cnxn)
logging.info('Done.')

logging.info('Summary Stats Output...')
med_var.to_sql("med_fields",sql_con, if_exists="replace")
phm_var.to_sql("phm_fields",sql_con, if_exists="replace")
logging.info('Done.')

sql_con.close()
cursor.close()
cnxn.close()