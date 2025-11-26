# -*- coding: utf-8 -*-
"""
Created on Mon May 10 10:49:20 2021

@author: camendol
"""
import csv
import calendar
import time
import datetime as dt
import collections
import glob

#Config/Set-up Values

delm='|'

report_vars='';
keep_vars='';

chunksize=100000
block_counter=0;

start = dt.datetime.now()

#Structure to capture freqs
var_freqs=collections.defaultdict(list)

#File Read
for _infile_ in glob.glob("J:\\Data_Warehouse\\Molina_TCOC\\MOL_MED_*"):
    with open(_infile_, 'r') as open_file:
        reader = csv.DictReader(open_file,delimiter=delm)
        header=reader.fieldnames
        
        for var in header:
            var_freqs[var]=collections.Counter() 
        
        for obs,row in enumerate(reader):
            block_counter+=1
            
            for col in row:
                var_freqs[col][row[col]] += 1
    
            if obs<6:
                print('OBS ',obs)
                print(row)
    
            if block_counter >=chunksize:
                block_counter=0
            if obs>9999999:break   
    
print("Read: J:\\Data_Warehouse\\Molina_TCOC\\MOL_MED_*")        
print ('{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, obs))
#print("Output: ",out_claims)      
print("Total Obs: ", obs)     
    
    
    #Data Profile Object - this is what the function should return
#     var_freqs=collections.defaultdict(list)
#     for var in profile_vars:
#         var_freqs[var]=collections.Counter() 
    
#     with open(_infile_, 'r') as open_file:
#         for obs,line in enumerate(open_file):  
#             block_counter+=1
#             #Weak rip off of SAS Program data vector idea
#             pdv=collections.OrderedDict(zip(header,line.split(data_delm)))
#             #Clean up null values
#             for column in header:
#                 if not(pdv[column]): pdv[column]='null'

#             #Simple frequency counter
#             #Dictionary of Dictionaries
#             for column in header:
#                 var_freqs[column][pdv[column]] += 1
        
#             #Replace with avro?
#             record=out_delm.join([pdv[column] for column in header])
#             if obs<6:
#                 print('Record ',obs)
#                 print(record)
    
#             if block_counter >=chunksize:
#                 block_counter=0
#             if obs>99:break    
         

# print("Read: J:\\Data_Warehouse\\Molina_TCOC\\MOL_MED_*")        
# print ('{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, obs))
# print("Output: ",out_claims)      
# print("Total Obs: ", obs)        