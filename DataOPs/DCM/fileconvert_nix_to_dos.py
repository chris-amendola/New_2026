# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 17:37:27 2020

@author: camendol
"""

import argparse
import logging
import os
import sys
from pathlib import Path

def nix_to_dos(src_file):
    windows_cr = '\r\n'
    nix_cr = '\n'

    tmp_file=str(src_file)+'.tmp'
    log_src_file=str(src_file)

    if os.path.exists(tmp_file):
        os.remove(tmp_file)
        
    #Adding in an external 'kill switch' since current automation
    #system 'force finish' doesn't terminate this process
    #Settiing check count equal to records counted at start allows for
    #kill switch check before any records are read - more or less
    
    kill_chk_ct=250000
    kill_chk_recs=250000
    kill_file=str(src_file)+'.kill'
    logging.info("--FILE CONVERT: "+log_src_file)
    with open(src_file, 'r') as infile, open(tmp_file, 'w') as outfile:
        for line in infile:
            if(kill_chk_ct==kill_chk_recs):
                
                if os.path.exists(kill_file):
                    outfile.close()
                    os.remove(tmp_file)
                    logging.info("--KILL FILE DETECTED: "+kill_file)
                    #Leave code reasonably gracefully
                    sys.exit(0)
                else: 
                    kill_chk_recs=0
            kill_chk_recs += 1    
            line.replace(nix_cr,windows_cr)
            outfile.write(line)

    if os.path.exists(tmp_file): 
        os.remove(src_file)
        os.rename(tmp_file,src_file)

# Initialize Loggging
logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)

logging.info('Argument Parsing:')

parser = argparse.ArgumentParser(description='Utility to convert *nix based line endings to Windows Line-endings')

parser.add_argument( '-s'
                    ,'--srcdir'
                    ,help='Path to raw data'
                    ,required=True)

parser.add_argument( '-m'
                    ,'--mask'
                    ,help='File extension mask - selects files of type'
                    ,required=True)

parser.add_argument( '-r'
                    ,'--sas_server'
                    ,help='SAS Server'
                    ,required=True)

parser.add_argument( '-d'
                    ,'--sasdrv'
                    ,help='SAS Driver'
                    ,required=True)

args = parser.parse_args()

logging.info("--Source Directory: "+args.srcdir)
logging.info("--File Mask: "+args.mask)
logging.info("--SAS Server: "+args.sas_server)
logging.info("--Server Drive: "+args.sasdrv)

#######
raw_path='\\\\'+args.sas_server+'\\'+args.sasdrv+args.srcdir
logging.info("Unix to dos conversion for data files: "+str(raw_path))
search_paths=list(Path(raw_path).glob("*."+args.mask))
for file in search_paths:
    nix_to_dos(file)
