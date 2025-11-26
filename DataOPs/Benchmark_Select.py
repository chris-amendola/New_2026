# -*- coding: utf-8 -*-
"""
Created on Sat Aug  3 09:30:54 2019

@author: camendol
"""


import pandas as pd
import numpy as np
import pyodbc
import matplotlib
import sys
import getopt
import logging

pyodbc.drivers()

logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

#Passed parms
# Server
# database

#Set some global constants
# Member column name
# Sample segment level
# Early Date Filter

logging.info('Establish SQL Server Connection....')
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};\
                      Server=DBSWP0776;\
                      database=II_INPUT70_BENCH_PD20180331_INC20171231;\
                      Trusted_Connection=yes")
logging.info('Done.')

cursor = cnxn.cursor()
# Generate State-Region Lookup table
state_region={  'AK':'West',
                'AL':'South',
                'AR':'South',
                'AZ':'West',
                'CA':'West',
                'CO':'West',
                'CT':'Northeast',
                'DC':'South',
                'DE':'South',
                'FL':'South',
                'GA':'South',
                'HI':'West',
                'IA':'Midwest',
                'ID':'West',
                'IL':'Midwest',
                'IN':'Midwest',
                'KS':'Midwest',
                'KY':'South',
                'LA':'South',
                'MA':'Northeast',
                'MD':'South',
                'ME':'Northeast',
                'MI':'Midwest',
                'MN':'Midwest',
                'MO':'Midwest',
                'MS':'South',
                'MT':'West',
                'NC':'South',
                'ND':'Midwest',
                'NE':'Midwest',
                'NH':'Northeast',
                'NJ':'Northeast',
                'NM':'West',
                'NV':'West',
                'NY':'Northeast',
                'OH':'Midwest',
                'OK':'South',
                'OR':'West',
                'PA':'Northeast',
                'RI':'Northeast',
                'SC':'South',
                'SD':'Midwest',
                'TN':'South',
                'TX':'South',
                'UT':'West',
                'VA':'South',
                'VT':'Northeast',
                'WA':'West',
                'WI':'Midwest',
                'WV':'South',
                'WY':'West',
                }
# Create a state to region lookup table
sql = "DROP TABLE region_lookup; create table region_lookup(state varchar(2),region varchar(10));"
logging.info('SQL Execute: '+sql)
cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

logging.info('Inserting region data into Lookup table...')
#The following is only accpetable for small such a small table
for state in (state_region.keys()):
    sql="insert into region_lookup (state,region) values ('"+state+"','"+state_region[state]+"')"
    cursor.execute(sql)
    cnxn.commit()
logging.info('Done.')

#Define member selection criteria
selection_criteria=[ ['DS5','Midwest','1','40']\
                    ,['DS5','South','1','30']\
                    ,['DS5','West','1','70']\
                    ,['DS5','Northeast','70','100']\

                    ,['DS1','Midwest','1','100']\
                    ,['DS10','Midwest','1','100']\
                    ,['DS11','Midwest','1','100']\
                    ,['DS12','Midwest','1','100']\
                    ,['DS16','Midwest','1','100']\
                    ,['DS3','Midwest','1','100']\
                    ,['DS4','Midwest','1','100']\
                    ,['DS6','Midwest','1','100']\
                    ,['DS7','Midwest','1','100']\
                    ,['DS9','Midwest','1','100']\

                    ,['DS1','South','1','100']\
                    ,['DS10','South','1','100']\
                    ,['DS11','South','1','100']\
                    ,['DS12','South','1','100']\
                    ,['DS16','South','1','100']\
                    ,['DS3','South','1','100']\
                    ,['DS4','South','1','100']\
                    ,['DS6','South','25','75']\
                    ,['DS7','South','1','100']\
                    ,['DS9','South','1','100']\

                    ,['DS1','West','1','100']\
                    ,['DS10','West','1','100']\
                    ,['DS11','West','1','100']\
                    ,['DS12','West','1','100']\
                    ,['DS16','West','1','100']\
                    ,['DS3','West','1','100']\
                    ,['DS4','West','1','100']\
                    ,['DS6','West','1','100']\
                    ,['DS7','West','1','100']\
                    ,['DS9','West','1','100']\

                    ,['DS1','Northeast','1','100']\
                    ,['DS10','Northeast','1','100']\
                    ,['DS11','Northeast','1','100']\
                    ,['DS12','Northeast','1','100']\
                    ,['DS16','Northeast','1','100']\
                    ,['DS3','Northeast','1','100']\
                    ,['DS4','Northeast','25','75']\
                    ,['DS6','Northeast','1','100']\
                    ,['DS7','Northeast','25','75']\
                    ,['DS9','Northeast','1','100']\
]

# Prepare a member based table with all required sampling elements
sql='DROP TABLE member_sample_prep_2\
     CREATE TABLE member_sample_prep_2(\
             [MEMBER] [varchar](32) NULL,\
             [sex] [float] NULL,\
             [DOB] [datetime] NULL,\
             [STATE_N] [varchar](2) NULL,\
             [ZIP_N] [varchar](10) NULL,\
             [MEM_USERDEF_3] [varchar](30) NULL,\
             [region_lookup][varchar](15) NULL,\
             [sample_seg][int] NULL,\
)'
logging.info('SQL Execute: '+sql)
cursor.execute(sql)
cnxn.commit()
logging.info('Done.')

# Filter Intial query for time range, and add region assignment via join.
sql = "select distinct  mem.member \
                        ,mem.sex \
                        ,mem.DOB \
                        ,mem.STATE_N \
                        ,mem.ZIP_N \
                        ,mem.MEM_USERDEF_3 \
                        ,lkup.region as region_lookup\
         from member_second as mem\
                left join\
              region_lookup as lkup\
            on mem.state_n=lkup.state\
         where end_dt > CAST('2015-12-31' AS DATE) ;"
# NOTE: Using ,CAST(Rand(mem.MEMBER) * 100 AS INT) + 1 as random_id_sample\
# has issues - DataError: ('22003', "[22003] [Microsoft][ODBC SQL Server Driver]
# [SQL Server]The conversion of the varchar value '2147549243' overflowed an int column. (248) (SQLFetch)")
# Chose PRNG approach occuring below.
logging.info('Begin PRNG sample segment assignments....')
rows = cursor.execute(sql)
columns = [column[0] for column in cursor.description]
results = []
for row in cursor.fetchall():
    row_dict=dict(zip(columns, row))

    seed_var=row_dict['member']
    seed_sum=0
    #Use Pseudo Random Number Generator to 'segment' member ids
    for pos in range( (len(seed_var)-2) , len(seed_var)+1 ):
        seed_sum=seed_sum+(ord(seed_var[(pos-1)]))* (8**((len(seed_var))-(pos)))

    row_dict['sample_seg']=seed_sum%100

    placeholder = ", ".join(["?"] * len(row_dict))
    # Place both PRNG segment and sample_id value back on DB table
    sql = "insert into {table} ({columns}) values ({values});"\
    .format(table='member_sample_prep_2', columns=","\
    .join(row_dict.keys()), values=placeholder)

    cursor.execute(sql, list(row_dict.values()))

cnxn.commit()
logging.info('Done.')

#Apply Selction criteria to assign members drop or keep
# TODO: Create an SQL table.
sql_pre="DROP TABLE sel_memb_list; "+\
        "select member,region_lookup,MEM_USERDEF_3,sex,DOB  "+\
        "into sel_memb_list "+\
        "from member_sample_prep_2 "+\
        "where "

sql_where=''
for counter,criteria in enumerate(selection_criteria,1):
    sql_where=sql_where\
             + "(    MEM_USERDEF_3='"+criteria[0]+"'"\
             + "      and region_lookup='"+criteria[1]+"'"\
             + "      and sample_seg>"+criteria[2]\
             + "      and sample_seg<="+criteria[3]+")"
    if counter<len(selection_criteria):
        sql_where=sql_where+" or "
    else:
        sql_where=sql_where+";"

sql_all=sql_pre+sql_where

logging.info('SQL Excute: '+sql_all)
cursor.execute(sql_all)
cnxn.commit()
logging.info('Done.')

logging.info('Select Claims(Medical and RX) for selected members')
logging.info('Begin SERVICEMED filter...')
# Filter MED service lines
sql="DROP TABLE SERVICEMED_SEL_2; "+\
    "SELECT med.*"+\
    "      ,sel.region_lookup,sel.MEM_USERDEF_3 "+\
    "INTO SERVICEMED_SEL_2 "+\
    "FROM SERVICEMED_second med "+\
    "INNER JOIN sel_memb_list sel "+\
    "         ON     med.MEMBER = sel.MEMBER "+\
    "WHERE med.dos > '12/31/2015';"
logging.info('SQL Excute: '+sql)
cursor.execute(sql)
cnxn.commit()
logging.info('DONE.')

logging.info('Begin SERVICERX filter...')
# Filter RX service lines
sql="DROP TABLE SERVICERX_SEL_2; "+\
    "SELECT phm.*"+\
    "      ,sel.region_lookup,sel.MEM_USERDEF_3 "+\
    "INTO SERVICERX_SEL_2 "+\
    "FROM SERVICERX_second phm "+\
    "INNER JOIN sel_memb_list sel "+\
    "         ON     phm.MEMBER = sel.MEMBER "+\
    "WHERE phm.dos > '12/31/2015';"
logging.info('SQL Excute: '+sql)
cursor.execute(sql)
cnxn.commit()
logging.info('DONE.')

logging.info('Begin MEMBER filter...')
# Filter member records
sql="DROP TABLE MEMBER_SEL_2; "+\
    "SELECT mem.*"+\
    "      ,sel.region_lookup "+\
    "INTO MEMBER_SEL_2 "+\
    "FROM MEMBER_second mem "+\
    "INNER JOIN sel_memb_list sel "+\
    "         ON     mem.MEMBER = sel.MEMBER "+\
    "where mem.end_dt > CAST('2015-12-31' AS DATE) ;"

logging.info('SQL Excute: '+sql)
cursor.execute(sql)
cnxn.commit()
logging.info('DONE.')

logging.info('Begin SUBSCRIBER filter...')
# Filter member records
sql="DROP TABLE SUBSCRIBER_SEL_2; "+\
    "SELECT sub.*"+\
    "INTO SUBSCRIBER_SEL_2 "+\
    "FROM SUBSCRIBER sub "+\
    "INNER JOIN MEMBER_SEL_2 sel "+\
    "         ON     sub.SUBSCRIBER_ID = sel.SUBSCRIBER_ID "+\
    "             and sub.eff_dt=sel.eff_dt"+\
	"		      and sub.end_dt=sel.end_dt;"

logging.info('SQL Excute: '+sql)
cursor.execute(sql)
cnxn.commit()
logging.info('DONE.')

logging.info('Closing database connection...')
cursor.close()
cnxn.close()
logging.info('DONE.')
logging.info('Program Complete.')
