# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 17:37:27 2020

@author: camendol
"""

import argparse
import os
from pathlib import Path

parser = argparse.ArgumentParser(description='Utility to convert *nix based line endings to Windows Line-endings')

parser.add_argument( '-r'
                    ,'--raw_path'
                    ,help='Path to raw data'
                    ,required=True)

args = parser.parse_args()

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
#######
print("Unix to dos conversion for data files: "+args.raw_path)
search_paths=list(Path(args.raw_path).rglob("*.[dD][aA][tT]"))
for file in search_paths:
    print(file)
    nix_to_dos(file)
