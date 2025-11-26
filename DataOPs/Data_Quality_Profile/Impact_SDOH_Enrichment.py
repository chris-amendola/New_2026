# -*- coding: utf-8 -*-
"""
Created on Tue May  5 12:44:36 2020

@author: camendol
"""

import pyodbc
import logging
import argparse
import subprocess
import sys

logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Social Determinants of Health IPro/II Member Enrichment')

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

parser.add_argument( '-a'
                    ,'--factor_set'
                    ,help='Enrichment Factor Set ID'
                    ,choices=['Base','UHC-Expanded']
                    ,default='Base'
                    ,required=False)
                    
parser.add_argument( '-x'
                    ,'--des_key_col'
                    ,help='Destination Table Key Column'
                    ,default='MEMBER'
                    ,required=False)

parser.add_argument( '-y'
                    ,'--fac_key_col'
                    ,help='Factors Table Key Column'
                    ,default='MEMBER'
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
                  ,7:['SDOH_FOOD_INSECURITY','[varchar](15) NULL']
                  ,8:['SDOH_OUT_OF_NETWORK','[smallint] NULL']
                  ,9:['SDOH_HOUSING_INSECURITY','[smallint] NULL']
                  ,10:['SDOH_FINANCIAL_INSECURITY','[smallint] NULL']
                  ,11:['SDOH_TRANSPORTATION_INSECURITY','[varchar](15) NULL']
                  ,}

if args.factor_set=='UHC-Expanded':
    add_columns_list={ 1:['SDOH_REASON_CODE','[varchar](25) NULL']
                      ,2:['SDOH_IMPUTED','[varchar](3) NULL']
                      ,3:['SDOH_HEALTH_OWNERSHIP_SEGMENT','[varchar](25) NULL']
                      ,4:['SDOH_SOCIAL_ISOLATION','[smallint] NULL']
                      ,5:['SDOH_PEI_PROGRAM_ENGAGEMENT','[smallint] NULL'] 
                      ,6:['SDOH_PEI_INBOUND_CALL','[smallint] NULL']
                      ,7:['SDOH_FOOD_INSECURITY','[varchar](15) NULL']
                      ,8:['SDOH_OUT_OF_NETWORK','[smallint] NULL']
                      ,9:['SDOH_HOUSING_INSECURITY','[smallint] NULL']
                     ,10:['SDOH_FINANCIAL_INSECURITY','[smallint] NULL']
                     ,11:['SDOH_TRANSPORTATION_INSECURITY','[varchar](15) NULL']
                     ,12:['SDOH_SOCIAL_VULNERABILITY','[smallint] NULL']
                     ,13:['SDOH_SOCIOECONOMIC_STATUS','[smallint] NULL']
                     ,14:['SDOH_LANGUAGE','[varchar](26) NULL']
                     ,15:['SDOH_ETHNICITY','[varchar](16) NULL']
                     ,16:['SDOH_EDUCATION_LEVEL','[varchar](26) NULL']
                     ,17:['ALINK_ADULTS_IN_HOUSEHOLD','[smallint] NULL']
                     ,18:['ALINK_PERSONS_IN_HOUSEHOLD','[smallint] NULL']
                     ,19:['ALINK_CHILDREN_IN_HOUSEHOLD','[smallint] NULL']
                     ,20:['ALINK_MARITAL_STATUS_HOUSEHOLD','[varchar](1) NULL']
                     ,21:['ALINK_MARITAL_STATUS_INDIVIDUAL','[varchar](1) NULL']
                     ,22:['ALINK_EST_HOUSEHOLD_INCOME','[varchar](19) NULL']
                     ,23:['ALINK_EST_DISCRET_INCOME_PERCENT','[smallint] NULL']
    }

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
        if row[0]==0:
            logging.info('No records Loaded for SDoH Factors file - TERMINATING!')
            logging.info('Typically this indicates a bad input file specification')
            sys.exit(1)
    logging.info('Done.')

    sql='''
        SELECT SUM(row_count) as [RowCount] 
          FROM sys.dm_db_partition_stats 
          WHERE object_id=OBJECT_ID('''+"'"+args.table+"'"+''') AND index_id IN ( 0, 1 );
        '''
    logging.info('SQL Execute: '+sql)

    sql_results=cursor.execute(sql)

    logging.info('Destination Table Record Count: ')
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
          sdoh ON  mem."+args.des_key_col+" = sdoh."+args.fac_key_col+";"
    
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
                select 
                  case 
                    when fac.'''+args.fac_key_col+''' is null 
                      then des.'''+args.des_key_col+'''
                    when des.'''+args.des_key_col+''' is null 
                      then fac.'''+args.fac_key_col+'''
                    else des.'''+args.des_key_col+''' 
                  end as common_id
                 ,des.'''+args.des_key_col+''' as des_key
                 ,fac.'''+args.fac_key_col+''' as fac_key  
                 ,case 
                   when fac.'''+args.fac_key_col+'''=des.'''+args.des_key_col+''' 
                     then 'MATCHED (Spans to SDoH)' 
                   when fac.'''+args.fac_key_col+''' is null 
                     then 'UN-MATCHED (Spans to SDoH)' 
                   when des.'''+args.des_key_col+''' is null 
                     then 'IDs from SDoH Factors Not in '''+args.table+''' '
                 else 'WTF' 
               end as category
          from sdoh_factors as fac full outer join '''+args.table+''' as des
               on des.'''+args.des_key_col+'''=fac.'''+args.fac_key_col+''') as fq
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

#python.exe \\apswp2286\W\camendol\SDOH\Impact_SDOH_Enrichment_002.py 
#--server DBSWP0871 
#--database SDoH_TEST 
#--table MEMBER_IPro_Data 
#--raw_path \\Apswp2286\I\Data_Warehouse\AMC_Enterprise\PD20210228\SDoH_Extract\ 
#--data_file 202103_UHC_CS_Indices.txt 
#--error_log \\Apswp2286\I\Data_Warehouse\AMC_Enterprise\PD20210228\SDoH_Extract\
#
#Optionals
##II only add columns
# add: --impact_type II --fields_only 
#
##IPro only add columns
# add:--fields_only
#
##II data
#add: --impact_type II
#
##Bad II Type
# add: --impact_type wrong
#
#Factor Sets
# add: --factor_set UHC-Expanded
#
#Destination Table Key
# add: --des_key_col MEMBER_ORIG
#python.exe \\apswp2286\W\camendol\SDOH\Impact_SDOH_Enrichment.py --server DBSWP0874 --database Americhoice11 --table MEMBER --raw_path \\Apswp2286\I\Data_Warehouse\AMC_Enterprise\PD20210228\SDoH_Extract\ --data_file 202103_UHC_CS_Indices.txt --error_log \\Apswp2286\I\Data_Warehouse\AMC_Enterprise\PD20210228\SDoH_Extract\ --factor_set UHC-Expanded