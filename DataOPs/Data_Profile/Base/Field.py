# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 09:40:05 2020

@author: camendol
"""
#Packages
import argparse
import logging
import pandas as pd
import sqlite3
import saspy

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
               +" WHERE "+column+" IS NULL "\
               +" GROUP BY "+by_var\
               +" \nUNION ALL\n "\
               +" SELECT "+by_var\
               +"      ,'"+column+"' as variable"\
               +"       ,"+column+" as value"\
               +"      ,count("+column+") as freq"\
               +"  FROM "+table\
               +"  WHERE "+column+" IS NOT NULL "\
               +"  GROUP BY "+column+","+by_var
 
    return sql+";"

#Main
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Data Quality Monitoring Fields Summary Mart Creation')

parser.add_argument( '-x'
                    ,'--sas_server'
                    ,help='SAS Server'
                    ,required=True)

parser.add_argument( '-i'
                    ,'--sas_drive'
                    ,help='SAS Drive'
                    ,required=True)

parser.add_argument( '-c'
                    ,'--client_name'
                    ,help='Client Identifier'
                    ,required=True)

parser.add_argument( '-y'
                    ,'--cycle'
                    ,help='Cycle'
                    ,required=True)

parser.add_argument( '-o'
                    ,'--output_dir'
                    ,help='Filepath to locate the output summary Db file'
                    ,required=True)
                    
parser.add_argument( '-t'
                    ,'--dataset'
                    ,help='Source Dataset'
                    ,default='BAD'
                    ,required=False)
                    
parser.add_argument( '-v'
                    ,'--by_var_list'
                    ,help='By-Group Dimensions'
                    ,default='contract'
                    ,required=False)
                    
#PASS in the column list required?
args = parser.parse_args()

logging.info("--Client: "+args.client_name)
logging.info("--Cycle: "+args.cycle)
logging.info("--Dataset: "+args.dataset)

logging.info('Activating SAS session....')

sas = saspy.SASsession(cfgfile='C:\\Users\\camendol\\AppData\\Local\\Continuum\\anaconda3\\Lib\\site-packages\\saspy\\personal.py')
sas.set_batch(True)
src_dir="\\\\"+args.sas_server+"\\"+args.sas_drive+"\\Data_Warehouse\\"+args.client_name+"\\PD"+args.cycle+"\\base"

lib_return=sas.saslib( "_src_"
                      , path=src_dir)

logging.info('Done.')

#Combine dataset files into single filehandle
sas_return=sas.submit('''
           
           proc sql;
             
             select '_src_.'!!strip(memname) 
               into :_src_datasets_
                 separated by " "
             from dictionary.tables
             where libname eq "_SRC_" 
               and memname like upcase("'''+args.dataset+'''")
               and memname not like '%_ADJ_%'
               and memname not like '%_VIEW'
            ;
           quit;
          
           data work.conc_data
            /view=work.conc_data;
             set &_src_datasets_.;
           run; 
           
           proc contents data=work.conc_data
                          out=work._conts_ 
                         noprint ;
           run;
           proc print data=work._conts_ noobs;
             var name;
           run;          
           ''',results='TEXT')
           
ds = sas.sasdata('conc_data', 'work')
p=ds.head()           

#Establish output Db file connection
#Send summary/aggregates to storage
sql_con = sqlite3.connect(args.output_dir+args.client_name+"_"+args.cycle+"_summ.sqlite")

by_vars=[args.by_var_list]
by_var=args.by_var_list

#Frequency Analysis
#Dynamic cols determination
#col_list=args.fq_cols
col_list=[ 'PCP_ORIG'
          ,'member'
          ,'PROV_AFFIL_MEM'
          ,'HMO_AT_RISK_MEM'
          ,'HMO_AT_RISK_MEM_orig'
          ,'PCP'
         ]

#Frequency analysis query construction and execution.
logging.info('Service Fields summarization...')
freq_query=freq_summ( 'work.conc_data'\
                     ,col_list\
                     ,by_var\
                     ,skip_list=['UNIQ_REC_ID','CUST_MED_PK_ID'])

#print('\n',freq_query,'\n')

sas_return2=sas.submit('''
        
      proc sql noprint;
        create table work._summ_ as '''+
        freq_query+''';
      quit;  
                                            
    ''')
logging.info('Done.')

logging.info('Summary Stats Output...')

fqs_sas=sas.sasdata('_summ_', 'work')    
    
fqs=fqs_sas.to_df()
fqs.to_sql( args.dataset.replace('%', '')+"_fqs"
            ,sql_con
            ,if_exists="replace")

logging.info('Done.')
    
sas.endsas() 
sql_con.close()