# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 17:44:30 2021

@author: camendol
"""
import sqlite3
import pandas as pd
import numpy as np
import subprocess
import logging
import pyodbc
from pathlib import Path
import sqlalchemy as sa

type='MS'

ms_server='DBSWP0879'
ms_db='Stage_1307'

lite_db = 'W:\\DVU_Impact_Data\\Molina\\PD20200131\\raw\\database.sqlite3'

table_name='member'
delimiter='|'

src_dir='W:\\DVU_Impact_Data\\Molina\\PD20200131\\raw\\'
src_file='mem_1307.txt.dlm'
_infile_=src_dir+src_file

out_dir='\\\\nasgw8315pn.ihcis.local\\Imp\\Users\\camendol\\DataOPs\\Data_Quality_Profile\\'

logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Schema Inference...')
logging.info('---Establish SQL Server Connection....')


if type=='MS':

    logging.info('MSSQL.')
    pyodbc.drivers()
    #Connect SQL Server Db
    # conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};\
    #                         Server="+ms_server+";\
    #                         database="+ms_db+";\
    #                         Trusted_Connection=yes")
                           
    engine=sa.create_engine('mssql+pyodbc://'+ms_server+'/'+ms_db+'?driver=SQL Server?Trusted_Connection=yes')  
    conn=engine.connect()    
                 
else:                  
    logging.info('SQLite')
    #Connect sqlite db
    conn = sqlite3.connect(lite_db)
    
#Read CSV
#--Header
with open(_infile_) as f:
    header = f.readline().rstrip('\n')
    
ordered_cols=header.split('|')  
  
#--Chunck of rows(add file sampling approach)
sample = pd.read_csv( _infile_
                     ,nrows=10000
                     ,sep='|'
                     ,low_memory=False
                     ,usecols=ordered_cols
                     ,dtype=str)

#Get max length of columns
measurer = np.vectorize(len)
cols = sample.select_dtypes(include=[object])
var_lens = dict( zip(cols, measurer(cols.values.astype(str))\
                 .max(axis=0)))
    
#Generate Create Table Statement
create_table_sql_inf=pd.io.sql.get_schema( sample
                                          ,table_name
                                          ,con=conn)

logging.info(create_table_sql_inf)


create_table_sql=create_table_sql_inf

#Run Create Table
if type=='MS':
    conn.execute("IF OBJECT_ID('dbo."+table_name+"', 'U') IS NOT NULL DROP TABLE dbo."+table_name+";")
    conn.execute(create_table_sql)
else:
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS "+table_name+";")
    conn.commit()
    c.execute(create_table_sql)
    conn.commit()

logging.info('Start Db Load')
#Now Bulk Load Raw data into table
if type=='MS':
    call=[ "bcp"
      ,ms_db+'.dbo.'+table_name   
      ,"IN"
      ,_infile_
      ,"-c"
      ,"-t"+delimiter
      ,"-S "+ms_server
      ,"-T"
      ,"-e"
      ,out_dir+table_name+'.err'
     ]

    skip_header='Y'
    if skip_header.upper()=='Y':
        call.append("-F 2")    

    logging.info(call)

    call_return=subprocess.run( call
                               ,capture_output=True)
    
    #logging.info(call_return)
    
else:    
    db_name = Path(lite_db).resolve()
    in_file = Path(_infile_).resolve()
    result = subprocess.run(['sqlite3',
                             str(db_name),
                            '-cmd',
                            '.separator '+"'|'",
                            '.import --skip 1 '+str(in_file).replace('\\','\\\\')
                                  +' '+table_name],
                        capture_output=True)

logging.info('Db Load Complete.')

#Get row count post bulk copy
logging.info('Check Table Load')
for num,var in enumerate(header.split('|')):
    print(num+1,':',var)
    