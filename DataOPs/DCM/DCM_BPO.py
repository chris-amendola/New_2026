# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 14:29:47 2019

@author: camendol
"""
#Packages
import argparse
import logging
import subprocess
import pyodbc
import os
import sys

# Initialize Loggging
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Opswise SQL Bulk Loader Utility')

parser.add_argument( '-s'
                    ,'--server'
                    ,help='II Input Server ID'
                    ,required=True)

parser.add_argument( '-d'
                    ,'--database'
                    ,help='II Input Database'
                    ,required=True)

parser.add_argument( '-t'
                    ,'--table'
                    ,help='Load Table Name'
                    ,required=True)

parser.add_argument( '-e'
                    ,'--error_log'
                    ,help='BCP Error Log Path'
                    ,required=True)

parser.add_argument( '-r'
                    ,'--raw_path'
                    ,help='Path to raw data'
                    ,required=True)

parser.add_argument( '-f'
                    ,'--data_file'
                    ,help='Raw Data File to Load'
                    ,required=True)

parser.add_argument( '-c'
                    ,'--delimiter'
                    ,help='*C*olumn Delimter'
                    ,default='|'
                    ,required=False)

parser.add_argument( '-l'
                    ,'--skip_header'
                    ,help='Skip Header Row'
                    ,choices=['Y','y','N','n']
                    ,default='Y'
                    ,required=False)

args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)
logging.info("--Table: "+args.table)
logging.info("--Error Log: "+args.error_log)
logging.info("--Raw Path: "+args.raw_path)
logging.info("--Data File: "+args.data_file)
logging.info("--Column Delimter: "+args.delimiter)
logging.info("--Skip Header: "+args.skip_header)

pyodbc.drivers()

logging.info('Establish SQL Server Connection....')
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};\
                      Server="+args.server+";\
                      database="+args.database+";\
                      Trusted_Connection=yes")

cursor = cnxn.cursor()

logging.info('Done.')

#Clear records from table in event of a re-run
logging.info('Clear Table: ')
logging.info('--'+args.table)

sql='TRUNCATE TABLE dbo.'+args.table+';'
logging.info('SQL Execute: '+sql)

sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

logging.info('Bulk Copier Call:')

call=[ "bcp"
      ,args.database+'.dbo.'+args.table   
      ,"IN"
      ,args.raw_path+args.data_file
      ,"-c"
      ,"-t"+args.delimiter
      ,"-S "+args.server
      ,"-T"
      ,"-e"
      ,args.error_log+args.table+".err"
      ]

if args.skip_header.upper()=='Y':
    call.append("-F 2")    

logging.info(call)

call_return=subprocess.run( call
                           ,capture_output=True)
logging.info(call_return)

#Check if the error log has lines
err_log_chk=os.stat(args.error_log+args.table+".err").st_size
logging.info('Error File Check: ')
logging.info(err_log_chk)
if err_log_chk > 0:
    sys.exit(1)

#Get row count post bulk copy
logging.info('Check Table Load')

sql='''SELECT SUM(row_count) as [RowCount] 
       FROM sys.dm_db_partition_stats 
       WHERE object_id=OBJECT_ID('''\
      +"'"+args.table\
      +"') AND index_id IN ( 0, 1 );"
                                 
logging.info('SQL Execute: '+sql)

sql_results=cursor.execute(sql)

logging.info('Record Count: ')
for row in cursor:
    logging.info('--'+str(row[0]))
logging.info('Done.')

#Close out Db connections
cursor.close
cnxn.close