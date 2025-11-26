# -*- coding: utf-8 -*-
"""
Created on Fri 08.30.2020

@author: camendol
"""
#python .\Provider_Dataop_Volume.py   
#--client_name Reliant
#--cycle 20210228
#--output_dir W:\camendol\SPC\data\ 
#--sas_server apswp2287
#--sas_drive J
#--dataset RMG_CUR_MEDCMS%
#--by_var_list PLAN_SOURCE
#--time_sum_col serv_dt
#--sum_by ['amt_pay','amt_req','amt_all','qty']
#--span_table False

#Packages
import argparse
import logging
import sqlite3
import saspy

#Loacl Functionso documentation available 
def summ_by_yr_mon_query( date_var=''\
                         ,sum_vars=''\
                         ,by_vars=''\
                         ,data_type=''
                         ,distinct_col='member'):
    
    sql_query_vars=''
    group_order_list=''
    
    for var in sum_vars:
        sql_query_vars=sql_query_vars+',sum('+var+') as '+var+'_sum'

    for by_var in by_vars:
        sql_query_vars=sql_query_vars+','+by_var+' '
        group_order_list=group_order_list+','+by_var+' '
           
    sql_query_prefix= '''select YEAR('''+date_var+''') as Yr
                               ,MONTH('''+date_var+''') as Mon
                               ,count(*) as numrows_sum
                               , count(distinct '''+distinct_col+''') as distinct_sum'''
                      
    sql_query_suffix = ' from '+data_type\
                      +' group by Yr, Mon '\
                      +group_order_list\
                      +' order by Yr, Mon '\
                      +group_order_list+';'                   
    
    return sql_query_prefix+sql_query_vars+sql_query_suffix

#Main
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Data Quality Monitoring Volumetrics Summary Mart Creation')

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

parser.add_argument( '-l'
                    ,'--time_sum_col'
                    ,help='Temporal Summary Column'
                    ,default='DOS'
                    ,required=False) 

parser.add_argument( '-b'
                    ,'--sum_by'
                    ,help='Summarize Dimension for Volumetrics'
                    ,nargs='+'
                    ,default=['amt_pay','amt_req','amt_all','qty']
                    ,required=False) 

parser.add_argument( '-e'
                    ,'--span_table'
                    ,help='Member or Subscriber Table Flag'
                    ,default='False'
                    ,required=False) 

parser.add_argument( '-q'
                    ,'--span_beg_col'
                    ,help='Span Begin Column'
                    ,default='EFF_DT'
                    ,required=False) 

parser.add_argument( '-r'
                    ,'--span_end_col'
                    ,help='Span End Column'
                    ,default='END_DT'
                    ,required=False) 

parser.add_argument( '-z'
                    ,'--distinct_unit_col'
                    ,help='Span column Member or other distinct Identifer'
                    ,default='Member'
                    ,required=False) 

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
           
           ''')
           
ds = sas.sasdata('conc_data', 'work')
p=ds.head()           

#Establish output Db file connection
#Send summary/aggregates to storage
sql_con = sqlite3.connect(args.output_dir+args.client_name+"_"+args.cycle+"_summ.sqlite")

#Volumetrics
# Basic Volumetrics- any of these could/should become set by arguments
by_vars=[args.by_var_list]
by_var=args.by_var_list

vol_date_col=args.time_sum_col
sum_by=args.sum_by

if args.span_table!='False':
    
    logging.info('Span Table Preparation')
    #Construct member months volumetrics
    sas_mons_spans='''
        data work.project_spans_months( keep='''+by_var+'''
                                             '''+args.distinct_unit_col+''' 
                                             '''+args.span_beg_col+''' 
				    					     '''+args.span_end_col+'''
                                       drop=span_beg_dt 
                                            span_end_dt 
                                            effective_date 
                                            term_date);
        
         set work.conc_data(rename=('''+args.span_beg_col+'''=effective_date
                                    '''+args.span_end_col+'''=term_date));
         
         format '''+args.span_beg_col+''' 
                '''+args.span_end_col+''' 
                span_beg_dt 
                span_end_dt yymmdd10.;

         rank_mem=_n_;

         span_beg_dt=intnx('month',effective_date,0,'s');
         span_end_dt=intnx('month',term_date,0,'s');
         /*Maybe limit span end date to current date?*/
         if span_end_dt >today() then span_end_dt=intnx('month',today(),0,'s');
  
         '''+args.span_beg_col+'''=span_beg_dt;
         '''+args.span_end_col+'''=intnx('month','''+args.span_beg_col+''',0,'e');
         output;
  
         do while ('''+args.span_end_col+''' < span_end_dt);
           
           '''+args.span_beg_col+'''=intnx('month','''+args.span_beg_col+''',1,'s');
           '''+args.span_end_col+'''=intnx('month','''+args.span_beg_col+''',0,'e');
           output;
    
         end;
       run;	
       proc sort data=work.project_spans_months
                 nodupkey;
           by '''+by_var+'''
              '''+args.distinct_unit_col+''' 
              '''+args.span_beg_col+'''     					   
              '''+args.span_end_col+''';
      run;
	'''
    
    sas_return_a=sas.submit(sas_mons_spans)
    
    vol_query='''
	    select '''+by_var+''' as '''+by_var+''' 
              ,put(MONTH(eff_dt),2.) as Mon
              ,put(YEAR(eff_dt),4.) as Yr
        ,count('''+args.distinct_unit_col+''') as dmos_sum
        ,count(distinct '''+args.distinct_unit_col+''') as mmos_sum
          from work.project_spans_months
          group by  Mon
                   ,Yr
				   ,'''+by_var+'''
          order by  Yr
                   ,Mon
				   ,'''+by_var+'''; 
     '''
         
else:
    #Construct service volumetrics queries
    vol_query=summ_by_yr_mon_query( date_var=args.time_sum_col\
                                   ,sum_vars=args.sum_by\
                                   ,data_type='work.conc_data'\
                                   ,by_vars=by_vars
                                   ,distinct_col=args.distinct_unit_col)

logging.info('Table Volumetric summarization...')

sas_return2=sas.submit('''
        
      proc sql noprint;
        create table work._summ_ as '''+
        vol_query+''';
      quit;  
                                            
    ''')

vol_sas=sas.sasdata('_summ_', 'work')    
    
vol=vol_sas.to_df()
vol.to_sql( args.dataset.replace('%', '')+"_vol"
            ,sql_con
            ,if_exists="replace")

logging.info('Done.')
    
sas.endsas() 
sql_con.close()