# -*- coding: utf-8 -*-
"""
Created on Fri 08.28.2020

@author: camendol
"""
#Packages
import argparse
import logging
import subprocess
import pyodbc
import os
import sys

#SAMPLE CLI Call:
#C:\Users\camendol\AppData\Local\Continuum\anaconda3\python .\Table_Extract_BCP.py 
#--database 'SDoH_TEST' 
#--server 'DBSWP0871' 
#--table SDOH_FACTORS 
#--out_path W:\camendol 
#--out_file LOOK_HERE 
#--delimiter ~

logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Opswise SQL Table Export Utility')

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

parser.add_argument( '-p'
                    ,'--out_path'
                    ,help='Path to raw data'
                    ,required=True)

parser.add_argument( '-f'
                    ,'--out_file'
                    ,help='filename for output data'
                    ,required=True)

parser.add_argument( '-c'
                    ,'--delimiter'
                    ,help='*C*olumn Delimter. Def "|"'
                    ,default='|'
                    ,required=False)

parser.add_argument( '-o'
                    ,'--observations'
                    ,help='Observations to return'
                    ,default='ALL'
                    ,required=False)

parser.add_argument( '-e'
                    ,'--extension'
                    ,help='Extension for Output File'
                    ,default='none'
                    ,required=False)

args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)
logging.info("--Table: "+args.table)
logging.info("--Out Path: "+args.out_path)
logging.info("--Out File: "+args.out_file)
logging.info("--Column Delimter: "+args.delimiter)

pyodbc.drivers()

logging.info('Establish SQL Server Connection....')
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};\
                      Server="+args.server+";\
                      database="+args.database+";\
                      Trusted_Connection=yes")

cursor = cnxn.cursor()

logging.info('Done.')

logging.info('Acquiring columns for export header.')

# Use a one line query to acquire the columns for a header line
sql='''
  SELECT top 1 *
    FROM '''+args.table   
 
sql_results=cursor.execute(sql)
columns = [column[0] for column in cursor.description]
header=args.delimiter.join(columns)
logging.info("Columns:")
logging.info(header)
    
logging.info('Done.')

##Close out Db connections
cursor.close
cnxn.close

logging.info('Bulk Copier Call:')

extension_resolve='.'+args.extension
if args.extension=='none':
    extension_resolve=''

pre_extract=args.out_path+'\\pre_'+args.out_file+'.tmp'
call=[ "bcp"
      ,args.database+'.dbo.'+args.table   
      ,"OUT"
      ,pre_extract
      ,"-c"
      ,"-t"+args.delimiter
      ,"-S "+args.server
      ,"-T"
      ,"-a 20480"
      ]

if args.observations.upper()!='ALL':
    call.append("-L "+args.observations) 

logging.info(call)

call_return=subprocess.run( call
                           ,capture_output=True
                           ,universal_newlines=True
                           ,)
#MAY HAVE TO PARSE THIS logging.info(call_return)
#print(call_return)

#PITA- Add header to data - BCP does not do this as part of its' native functions
final_file=args.out_path+'\\'+args.out_file+extension_resolve
#Open final output
if os.path.exists(final_file):
        os.remove(final_file)
        
with open(pre_extract, 'r') as infile, open(final_file, 'w') as outfile:
    #Header
    outfile.write(header+'\n')
    #Data
    for line in infile:
        outfile.write(line)
#House-keeping   
if os.path.exists(pre_extract): 
        os.remove(pre_extract)        

