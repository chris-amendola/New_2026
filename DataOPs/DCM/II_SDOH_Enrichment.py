# -*- coding: utf-8 -*-
"""
Created on Tue May  5 12:44:36 2020

@author: camendol
"""

import pyodbc
import logging
import argparse
import subprocess
import pandas as pd

logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Social Determinants of Health IPro Member Enrichment')

parser.add_argument( '-s'
                    ,'--server'
                    ,help='Impact Input Server ID'
                    ,required=True)

parser.add_argument( '-d'
                    ,'--database'
                    ,help='Impact Input Database'
                    ,required=True)

parser.add_argument( '-t'
                    ,'--table'
                    ,help='Impact Enrichment Destination Table'
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

parser.add_argument( '-e'
                    ,'--error_log'
                    ,help='BCP Error Log Path'
                    ,required=True)

parser.add_argument( '-o'
                    ,'--fields_only'
                    ,help='Create Fields Only'
                    ,action='store_true')

parser.add_argument( '-i'
                    ,'--impact_type'
                    ,help='Which Impact Product are we using (IPro/II)?'
                    ,choices=['II','IPro']
                    ,default='IPro'
                    ,required=False)

args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)
logging.info("--Raw Path: "+args.raw_path)
logging.info("--Data File: "+args.data_file)
logging.info("--Column Delimter: "+args.delimiter)
logging.info("--Skip Header: "+args.skip_header)
logging.info("--Error Log: "+args.error_log)
logging.info("--Impact Product Type: "+args.impact_type)

if (args.fields_only==True):
    logging.info("NOTE: Adding Fields Only to destination table.")
  
#Merge Key in the future    
# Knowldege of the involved columns for indices is localized
add_columns_list={ 1:['SDOH_REASON_CODE','[varchar](25) NULL']
                  ,2:['SDOH_IMPUTED','[varchar](3) NULL']
                  ,3:['SDOH_HEALTH_OWNERSHIP_SEGMENT','[varchar](25) NULL']
                  ,4:['SDOH_SOCIAL_ISOLATION','[smallint] NULL']
                  ,5:['SDOH_PEI_PROGRAM_ENGAGEMENT','[smallint] NULL'] 
                  ,6:['SDOH_PEI_INBOUND_CALL','[smallint] NULL']
                  ,7:['SDOH_OUT_OF_NETWORK','[smallint] NULL']
                  ,8:['SDOH_HOUSING_INSECURITY','[smallint] NULL']
                  ,9:['SDOH_FINANCIAL_INSECURITY','[smallint] NULL']
                  ,10:['SDOH_FOOD_INSECURITY','[varchar](15) NULL']
                  ,11:['SDOH_TRANSPORTATION','[varchar](15) NULL']}

#Impact Intelligence needs these fields to be 'custom'
if(args.impact_type=='II'):
    for i in range(len(add_columns_list)):
        add_columns_list[i+1][0]='CUST_'+add_columns_list[i+1][0]
    
pyodbc.drivers()

logging.info('Establish SQL Server Connection....')
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};\
                      Server="+args.server+";\
                      database="+args.database+";\
                      Trusted_Connection=yes")

cursor = cnxn.cursor()

logging.info('Done.')

sql='''
IF OBJECT_ID('SDOH_FACTORS') IS NOT NULL
	BEGIN
		DROP TABLE SDOH_FACTORS;
	END 
'''

logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

sql='''
CREATE TABLE  SDOH_FACTORS
(
    [MEMBER][varchar](32),
	[FIRSTNAME][varchar](50) NULL,
	[LASTNAME][varchar](50) NULL,
	[BIRTHDATE][smalldatetime] NULL,
    [GENDER][varchar](1) NULL,
	[ADDRESS][varchar](50) NULL,
	[CITY][varchar](50) NULL,
	[STATECODE][varchar](10) NULL,
	[POSTALCODE][varchar](10) NULL,
'''
for i in range(len(add_columns_list)):
    sql=sql+'['+add_columns_list[i+1][0]+']'+add_columns_list[i+1][1]+',\n'
sql=sql+');'    
    
logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

if (args.fields_only==False):
    logging.info('Bulk Copier Call:')

    call=[ "bcp"
          ,args.database+'.dbo.SDOH_FACTORS'   
          ,"IN"
          ,args.raw_path+args.data_file
          ,"-c"
          ,"-t"+args.delimiter
          ,"-S "+args.server
          ,"-T"
          ,"-e"
          ,args.error_log+"sdoh_factors.err"
          ]

    if args.skip_header.upper()=='Y':
        call.append("-F 2")    

    logging.info(call)

    call_return=subprocess.run( call
                               ,capture_output=True)

    #Get row count post bulk copy
    logging.info('Check Table Load')

    sql='''
        SELECT SUM(row_count) as [RowCount] 
          FROM sys.dm_db_partition_stats 
          WHERE object_id=OBJECT_ID('SDOH_FACTORS') AND index_id IN ( 0, 1 );
         '''       
                                 
    logging.info('SQL Execute: '+sql)

    sql_results=cursor.execute(sql)

    logging.info('SDOH Factors File Record Count: ')
    
    for row in cursor:
        logging.info('--'+str(row[0]))
    logging.info('Done.')

    sql='''
        SELECT SUM(row_count) as [RowCount] 
          FROM sys.dm_db_partition_stats 
          WHERE object_id=OBJECT_ID('MEMBER') AND index_id IN ( 0, 1 );
        '''
    logging.info('SQL Execute: '+sql)

    sql_results=cursor.execute(sql)

    logging.info('MEMBER Table Record Count: ')
    for row in cursor:
        logging.info('--'+str(row[0]))
    logging.info('Done.')

#Clear any exisiting SDOH Factor columns - support re-runs
sql='';
for i in range(len(add_columns_list)):
    sql=sql+"\t  IF EXISTS (SELECT 1\n\
                           FROM  INFORMATION_SCHEMA.COLUMNS\n\
                           WHERE TABLE_NAME = '"+args.table+"'\n\
                           AND COLUMN_NAME = '"+add_columns_list[i+1][0]+"'\n\
                           AND TABLE_SCHEMA='DBO')\n\
              BEGIN\n\
                ALTER TABLE "+args.table+"\n\
                DROP COLUMN "+add_columns_list[i+1][0]+"\n\
              END\n\n"
      
logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

###HERE
sql='ALTER TABLE '+args.table+'\n\
     ADD '
for i in range(len(add_columns_list)):
    if (i==0):
        sql=sql+'['+add_columns_list[i+1][0]+']'+add_columns_list[i+1][1]+'\n'
    else:
        sql=sql+',['+add_columns_list[i+1][0]+']'+add_columns_list[i+1][1]+'\n'
sql=sql+";"    

logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

sql="UPDATE "+args.table+"\
  SET \n"
 
for i in range(len(add_columns_list)):
    if (i==0):
        sql=sql+args.table+"."+add_columns_list[i+1][0]+"=sdoh."+add_columns_list[i+1][0]+" \n"  
    else:
        sql=sql+','+args.table+"."+add_columns_list[i+1][0]+"=sdoh."+add_columns_list[i+1][0]+" \n"
         
sql=sql+" FROM "+args.table+" as mem INNER JOIN SDOH_FACTORS as\
          sdoh ON  mem.member = sdoh.member;"
    
logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

if (args.fields_only==False):
    logging.info('Check Match Rate between factors and MEMBER')
    ref_match='''
        select fq.category
              ,count(fq.category) as count
          from(      
               select case
                 when fac.member is null then mbr.member
                 when mbr.member is null then fac.member
                 else mbr.member
               end as common_id
              ,mbr.member as mbr_mem
              ,fac.member as fac_mem
              ,case
                 when fac.member=mbr.member then 'MATCHED (Spans to SDoH)'
                 when fac.member is null then 'UN-MATCHED (Spans to SDoH)'
                 when mbr.member is null then 'IDs from SDoH Factors Not in Member'
                 else 'WTF' 
               end as category
          from sdoh_factors as fac 
                 full outer join 
               '''+args.table+''' as mbr
               on mbr.member=fac.member) as fq
          group by fq.category     
    '''
      
    cursor.execute(ref_match)
    
    columns = [column[0] for column in cursor.description]
    results = [columns] + [row for row in cursor.fetchall()]

    for result in results:
        logging.info(result)

#Close out Db connections
cursor.close
cnxn.close

#TEST CASES:
##II only add columns
#python.exe \\tsclient\C\Users\camendol\Documents\DataOps-master\II_SDOH_Enrichment.py --server DBSWP0871 --database SDoH_TEST --table MEMBER_II_COLS --raw_path \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --data_file 202012_THP_WV_Indices.txt --error_log \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --impact_type II --fields_only 
#
##IPro only add columns
#python.exe \\tsclient\C\Users\camendol\Documents\DataOps-master\II_SDOH_Enrichment.py --server DBSWP0871 --database SDoH_TEST --table MEMBER_IPro_COLS --raw_path \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --data_file 202012_THP_WV_Indices.txt --error_log \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --fields_only
#
##II data
#python.exe \\tsclient\C\Users\camendol\Documents\DataOps-master\II_SDOH_Enrichment.py --server DBSWP0871 --database SDoH_TEST --table MEMBER_II_Data --raw_path \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --data_file 202012_THP_WV_Indices.txt --error_log \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --impact_type II
#
##IPro data
#python.exe \\tsclient\C\Users\camendol\Documents\DataOps-master\II_SDOH_Enrichment.py --server DBSWP0871 --database SDoH_TEST --table MEMBER_IPro_Data --raw_path \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --data_file 202012_THP_WV_Indices.txt --error_log \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\   
#
##Bad II Type
#python.exe \\tsclient\C\Users\camendol\Documents\DataOps-master\II_SDOH_Enrichment.py --server DBSWP0871 --database SDoH_TEST --table MEMBER_II_Data --raw_path \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --data_file 202012_THP_WV_Indices.txt --error_log \\Apswp2286\h\Data_Warehouse\THP_WV\PD20201130\SDoH_Extract\Do_not_use\ --impact_type wrong

