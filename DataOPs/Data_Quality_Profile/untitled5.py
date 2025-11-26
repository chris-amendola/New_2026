# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 16:48:05 2021

@author: camendol
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("APSWP2286") \
    .appName("SimpleTests") \
    .getOrCreate()
 
df = spark.read.options(delimiter='|')\
      .csv("W:\\DVU_Impact_Data\\Molina\\PD20200131\\raw\\clm_1307.txt.dlm")
df.printSchema()
spark.close
