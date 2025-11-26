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

parser.add_argument( '-e'
                    ,'--error_log'
                    ,help='BCP Error Log Path'
                    ,required=True)

args = parser.parse_args()

logging.info("--Server: "+args.server)
logging.info("--Database: "+args.database)
logging.info("--Raw Path: "+args.raw_path)
logging.info("--Data File: "+args.data_file)
logging.info("--Column Delimter: "+args.delimiter)
logging.info("--Skip Header: "+args.skip_header)
logging.info("--Error Log: "+args.error_log)

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
    
    [SDOH_HEALTH_OWNERSHIP][varchar](25) NULL,
    [SDOH_SOCIAL_ISOLATION][smallint] NULL,
    [SDOH_ENGAGEMENT_PROGRAM][smallint] NULL,
    [SDOH_ENGAGEMENT_INBOUND_CALL][smallint] NULL,
    [SDOH_OUT_OF_NETWORK][smallint] NULL,
    [SDOH_HOUSING_SECURITY][smallint] NULL,
    [SDOH_FINANCIAL_SECURITY][smallint] NULL,
    [SDOH_FOOD_SECURITY][varchar](15) NULL,
    [SDOH_MATCH_REASON_CODE][varchar](25) NULL,
    [SDOH_IMPUTED][varchar](3) NULL
    
);
'''
logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

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
#TODO: Should this be distinct members?
                              
logging.info('SQL Execute: '+sql)

sql_results=cursor.execute(sql)

logging.info('MEMBER Table Record Count: ')
for row in cursor:
    logging.info('--'+str(row[0]))
logging.info('Done.')

#Clear any exisiting SDOH Factor columns - support re-runs
sql='''
IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_ENGAGEMENT_PROGRAM'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_ENGAGEMENT_PROGRAM
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_HEALTH_OWNERSHIP'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_HEALTH_OWNERSHIP                    
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_SOCIAL_ISOLATION'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_SOCIAL_ISOLATION
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_ENGAGEMENT_INBOUND_CALL'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_ENGAGEMENT_INBOUND_CALL  
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_FOOD_SECURITY'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_FOOD_SECURITY
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_MATCH_REASON_CODE'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_MATCH_REASON_CODE
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_HOUSING_SECURITY'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_HOUSING_SECURITY
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_MATCH_REASON_CODE'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_MATCH_REASON_CODE
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_IMPUTED'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_IMPUTED
  END
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_FINANCIAL_SECURITY'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_FINANCIAL_SECURITY
  END
  
  IF EXISTS (SELECT 1
               FROM   INFORMATION_SCHEMA.COLUMNS
               WHERE  TABLE_NAME = 'MEMBER'
                      AND COLUMN_NAME = 'SDOH_OUT_OF_NETWORK'
                      AND TABLE_SCHEMA='DBO')
  BEGIN
      ALTER TABLE MEMBER
        DROP COLUMN SDOH_OUT_OF_NETWORK
  END
'''

logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

sql='''
ALTER TABLE MEMBER
ADD [SDOH_HEALTH_OWNERSHIP][varchar](25) NULL,
	[SDOH_SOCIAL_ISOLATION][smallint] NULL,
	[SDOH_ENGAGEMENT_PROGRAM][smallint] NULL,
	[SDOH_ENGAGEMENT_INBOUND_CALL][smallint] NULL,
	[SDOH_FOOD_SECURITY][varchar](15) NULL,
	[SDOH_OUT_OF_NETWORK][smallint] NULL,
	[SDOH_HOUSING_SECURITY][smallint] NULL,
    [SDOH_FINANCIAL_SECURITY][smallint] NULL,
	[SDOH_MATCH_REASON_CODE][varchar](25) NULL,
	[SDOH_IMPUTED][varchar](3) NULL;
'''

logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

sql='''
UPDATE MEMBER
  SET MEMBER.SDOH_IMPUTED=sdoh.SDOH_IMPUTED
     ,MEMBER.SDOH_MATCH_REASON_CODE=sdoh.SDOH_MATCH_REASON_CODE
     ,MEMBER.SDOH_HOUSING_SECURITY=sdoh.SDOH_HOUSING_SECURITY
     ,MEMBER.SDOH_OUT_OF_NETWORK=sdoh.SDOH_OUT_OF_NETWORK
     ,MEMBER.SDOH_FOOD_SECURITY=sdoh.SDOH_FOOD_SECURITY
     ,MEMBER.SDOH_ENGAGEMENT_INBOUND_CALL=sdoh.SDOH_ENGAGEMENT_INBOUND_CALL
     ,MEMBER.SDOH_ENGAGEMENT_PROGRAM=sdoh.SDOH_ENGAGEMENT_PROGRAM
     ,MEMBER.SDOH_SOCIAL_ISOLATION=sdoh.SDOH_SOCIAL_ISOLATION
     ,MEMBER.SDOH_HEALTH_OWNERSHIP=sdoh.SDOH_HEALTH_OWNERSHIP
     ,MEMBER.SDOH_FINANCIAL_SECURITY=sdoh.SDOH_FINANCIAL_SECURITY
  FROM MEMBER mem
      INNER JOIN   
             SDOH_FACTORS sdoh
      ON  mem.member = sdoh.member
'''
logging.info('SQL Execute: '+sql)
sql_results=cursor.execute(sql)
cnxn.commit()
logging.info('Done.')
logging.info('Check Match Rate between factors and MEMBER')
ref_match=pd.read_sql_query('''
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
               member as mbr
               on mbr.member=fac.member) as fq
          group by fq.category     
    '''
    , cnxn)      
logging.info(ref_match)

#Close out Db connections
cursor.close
cnxn.close