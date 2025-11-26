# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 17:37:27 2020

@author: camendol
"""

import argparse
import logging
import os
from pathlib import Path

def nix_to_dos(src_file):
    windows_cr = '\r\n'
    nix_cr = '\n'

    tmp_file=str(src_file)+'.tmp'

    if os.path.exists(tmp_file):
        os.remove(tmp_file)

    
    with open(src_file, 'r') as infile, open(tmp_file, 'w') as outfile:
        for line in infile:
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
print("Unix to dos conversion for data files: ",raw_path)
search_paths=list(Path(raw_path).rglob("*"+args.mask))
for file in search_paths:
    print(file)
    nix_to_dos(file)
