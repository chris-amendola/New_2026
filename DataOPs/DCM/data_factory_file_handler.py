# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 17:37:27 2020

@author: camendol
"""
#Last test cli: C:\Users\camendol\AppData\Local\Continuum\anaconda3\python 
#  \\nasgw8315pn.ihcis.local\imp\Users\camendol\DataOPs\DCM\data_factory_file_handler.py 
# --srcdir \Data_Warehouse\_H623622_test\files\ 
# --sasdrv N 
# --sas_server APSWP2286 
# --batch 43249 
# --hnumber H623622 
# --cycle 20200630 

import argparse
import logging
import re
import os
import sys
from pathlib import Path

# Initialize Loggging
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Data Factory File Handling Component')

parser.add_argument( '-s'
                    ,'--srcdir'
                    ,help='Path to raw data'
                    ,required=True)

parser.add_argument( '-r'
                    ,'--sas_server'
                    ,help='SAS Server'
                    ,required=True)

parser.add_argument( '-d'
                    ,'--sasdrv'
                    ,help='SAS Driver'
                    ,required=True)

parser.add_argument( '-b'
                    ,'--batch'
                    ,help='Batch Id Number'
                    ,required=True)

parser.add_argument( '-n'
                    ,'--hnumber'
                    ,help='Humedica Id Number'
                    ,required=True)

parser.add_argument( '-c'
                    ,'--cycle'
                    ,help='II Cycle Identifier'
                    ,required=True)

args = parser.parse_args()

logging.info("--Source Directory: "+args.srcdir)
logging.info("--SAS Server: "+args.sas_server)
logging.info("--Server Drive: "+args.sasdrv)
logging.info("--CYCLE: "+args.cycle)

#######
ignore_files=['bcp_logs','archive','hold','temp','unknown','working','IVMQCFiles']

#Non-user configured list of specified DCM 7.2 file pattern templates
#Batch and HNUM are generalized for re-useablity and to be able
#to validate batches being deployed

file_pattern_list=['{batch}_{hnum}_{hnum}_ii_monthly_control_totals_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_ACCT_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_AT_RISK_STATUS_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_BENEFIT_PLAN_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_BIZ_SEGMENT_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_CONTRACT_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_CONTRACT_TYPE_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_COVERAGE_STATUS_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_DENIED_IND_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_DRG_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_EMPLOYEE_TYPE_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_MEM_PEER_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_MEM_USERDEF_1_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_MEM_USERDEF_2_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_MEM_USERDEF_3_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_MEM_USERDEF_4_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_NETWORK_PAID_STATUS_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_PEER_GRP_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_POS_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_PROD_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_PROVAFFIL_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_PROVIDER_STATUS_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_PROV_PEER_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_PROV_USERDEF_2_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_RISK_TYPE_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_SPEC_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_SPEC_RX_N_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_CUSTOM_TOS_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_LABDATA_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_MEMBER_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_PREMIUM_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_PROVIDER_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_SERVICEMED_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_SERVICERX_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_SUBSCRIBER_PD\d{8}.dat$'
,'{batch}_{hnum}_monthly_payer_PD\d{8}_job.properties$'
]

raw_path='\\\\'+args.sas_server+'\\'+args.sasdrv+args.srcdir
move_path='\\\\'+args.sas_server+'\\'+args.sasdrv+args.srcdir+'\\'+args.cycle+'\\'

#####

#Housekeeping required to be locate control totals
#--Have to find control totals that get moved into special direct
cnt_tot_pattern='{batch}_{hnum}_{hnum}_ii_monthly_control_totals_PD\d{8}.dat$'
cnt_tot_path='\\\\'+args.sas_server+'\\'+args.sasdrv+args.srcdir+'\\control_totals'

pat_sub=re.sub('{hnum}',args.hnumber,cnt_tot_pattern)
pat_sub=re.sub('{batch}',args.batch,pat_sub)

logging.info('Scan Control Total Data Location: ')
logging.info(cnt_tot_path)
logging.info('Match Pattern: ')
logging.info(pat_sub)

for pth, dir, files in os.walk(cnt_tot_path):
        for xfile in files:
            if re.match(pat_sub,xfile):
                logging.info('Control Total File: ')
                logging.info(pat_sub)
                #Move control total to all-files location
                os.rename(Path(pth+'\\'+xfile),Path(raw_path+'\\'+xfile))


#End House Keeping
                
search_paths=list(Path(raw_path).glob("*.*"))

file_chk_results={}
pattern_chk_results={}
file_rename_map={}

# Going to check for unspecified files in raw directory
# and check for required file patterns not found.
# Looking for both 'extra' files, and unmatched required files
for file in search_paths:
    file_name=str(Path(file).name)
   
    if file_name not in(ignore_files):
        file_chk_results[file_name]='NONE'
        for match_pattern in file_pattern_list:
            #Match by batch and h-number, df does not seem to match our cycle dates
            match_sub=re.sub('{hnum}',args.hnumber,match_pattern)
            match_sub=re.sub('{batch}',args.batch,match_sub)
        
            if match_sub not in pattern_chk_results:
                pattern_chk_results[match_sub]='NONE'
        
            if re.match(match_sub,file_name):
                file_chk_results[file_name]=match_sub
                file_rename_map[file_name]=re.sub('\d{8}',args.cycle,file_name)
                pattern_chk_results[match_sub]=file_name
#for done.
                
logging.info('Scan for Un-Specified Files...') 
unspecified=0           
for res_file in file_chk_results:
    if file_chk_results[res_file]=='NONE':
        logging.info(res_file)
        unspecified=1
logging.info('Scan for Un-Specified Files Complete.') 
      
logging.info('\n')

logging.info('Scan for Required Files... ') 
unfound=0
for res_file in pattern_chk_results:
    if pattern_chk_results[res_file]=='NONE':
        logging.info(pattern_chk_results[res_file]+' >>> '+res_file)
        unfound=1
logging.info('Scan for Required Files Complete ') 
      
if unfound or unspecified:
    logging.info('ERR: Specified file not found')
    #Shut it down
    sys.exit(1)
      
logging.info('Files moves and renames to correct cycle...')
#Create Destination directory
logging.info('Create Path: '+move_path) 
os.umask(0)
os.mkdir(move_path)
os.mkdir(move_path+'\\bcp_logs')

for raw_file in file_rename_map:
    logging.info(file_rename_map[raw_file])
    os.rename(Path(raw_path+raw_file),Path(move_path+file_rename_map[raw_file])) 
logging.info('Files renames to correct cycle Complete.')               
