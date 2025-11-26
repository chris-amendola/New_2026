# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 14:29:47 2019

@author: camendol
"""

#Packages
import sys
import argparse
import logging
import pyodbc
import csv

# Initialize Loggging
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Opswise DCM Control Total utility')

parser.add_argument( '-s'
                    ,'--server'
                    ,help='II Input Server ID'
                    ,required=True)

parser.add_argument( '-d'
                    ,'--database'
                    ,help='II Input Database'
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
#Get row count 
logging.info('Acquire DB Table Record counts: ')

# Get all user tables from Dbs
sql='''SELECT OBJECT_NAME(object_id) as [Table],row_count
       FROM sys.dm_db_partition_stats
       WHERE index_id in (0,1)
       AND objectproperty(object_id, 'IsUserTable') = 1;'''
logging.info(sql)
sql_results=cursor.execute(sql)

#Close out Db connections
cursor.close
cnxn.close

#Create Table row counts lookup dictionary
table_counts={}
for count_row in cursor:
    table_counts[count_row[0]]=count_row[1]

# Prep collection of mismatched table counts to control totals
failed_tables=[]  

infile=args.raw_path+args.data_file
logging.info('Read Control Total File: '+infile)
# Read control total file 
with open(infile, 'r') as cntrl_file: 
    # Create a csv/delimited reader object 
    file_reader = csv.reader(cntrl_file,delimiter='|')
  
    # Process control total file line by line 
    for line_pos in file_reader:
        #Deconstruct file name into list of elements - based on '_'
        #Drop process, hnumber, 'monthly' and 'payer' from the list - first 4 elements
        #Remove the '.dat'
        base_file=line_pos[0].split('/')[-1:]
        drop_ext=(base_file[0].split('.'))[0]
        table_key=('_'.join(drop_ext.split('_')[4:-1]))
        
        if int(table_counts[table_key])!=int(line_pos[1]):
            failed_tables.append((table_key,str(table_counts[table_key]),line_pos[0],str(line_pos[1])))

logging.info('Comparing control totals to row counts: ')

if len(failed_tables)>0:
    logging.info('Control Total Mismatches: ')
    #Report by row iteration
    for fail_row in failed_tables:
        report_list=( '    Table:',fail_row[0]\
                     ,'Rows:',str(fail_row[1])\
                     ,'Datafile:',fail_row[2]\
                     ,'Line Count:',str(fail_row[3]))
        
        logging.info(' '.join(report_list))
    sys.exit(1)    
else:
    logging.info('Control Totals all Matching.')
    
#Close out Db connections
cursor.close
cnxn.close