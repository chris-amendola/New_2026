# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 10:32:18 2021

@author: camendol
"""
#import dask.multiprocessing
#dask.config.set(scheduler='processes')
import dask.dataframe as dd
import logging

logging.basicConfig( format='%(asctime)s %(message)s'\
                    ,datefmt='%m/%d/%Y %I:%M:%S %p'\
                    ,level=logging.INFO)
 

src_dir='W:\\DVU_Impact_Data\\Molina\\PD20200131\\raw\\'
src_file='clm_1307.txt.dlm'
_infile_=src_dir+src_file

logging.info('Generate Raw File Reading solution...')
df = dd.read_csv( _infile_
                 ,sep='|'
                 ,low_memory=False
                 ,dtype=str
                 ,encoding='latin1')
dt=df.dtypes
native_dtypes=dt.to_dict()

df.compute()

logging.info('Done.')

logging.info('Frequency NULL')
null_freq=df.DATE_OF_SERVICE_BEG.isnull().sum().compute()
logging.info('Done.')

logging.info('Frequency Values.')
freq=df['DATE_OF_SERVICE_BEG'].value_counts().compute()

logging.info('Done.')    

# df['Yr_Mon']=dd.to_datetime( df['DATE_OF_SERVICE_BEG'].str[0:10]
#                             ,format='%Y-%m-%d')\
#                .map(lambda dt: dt.replace(day=1))

# freq=df['Yr_Mon'].value_counts().compute()
# print(freq)

# logging.info('COMPUTE...')
# logging.info(num_par)
# df.loc[ :,['DISCHARGE_DATE'
#        ,'ELIG_INPAT_DAYS'
#        ,'INELIG_INPAT_DAYS'
#        ,'NATURE_HSP_ADMISSION'
#        ,'PAT_STATUS_CODE'
#        ,'TRAUMA_INDICATOR','Yr_Mon']].groupby('Yr_Mon').count().compute()
# logging.info('Done.')

#Must have pyarrow installed
#df.to_parquet('W:\\DVU_Impact_Data\\Molina\\PD20200131\\raw\\')

# logging.info('Db Load Begin....')
# df.to_sql( 'dask_test'
#           ,uri=uri_string 
#           ,index=False)

# logging.info('Done.')

# conn.close()