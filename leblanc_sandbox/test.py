#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

import os
import collections

def blah(row):
    row[0] = row[0].replace(" School District",'')
    return row

schoolDistrictPopCsvPath = r"../data/WAOFM_-_SAEP_-_School_District_Population_Estimates__2000-2016.csv"
zipToCityCsvPath = r"../data/zip_code_database.csv"

#load school district population data
parseCsv = lambda line : line.split(',')
sc = SparkContext()
popRdd = sc.textFile(schoolDistrictPopCsvPath).map(parseCsv).map(lambda line : [line[0],line[4:-5]]).map(blah)
cityToZipRdd = sc.textFile(zipToCityCsvPath).map(parseCsv).map(lambda line : (line[3],line[0]))
zipToPopRdd = cityToZipRdd.join(popRdd).map(lambda line : [line[1][0]]+line[1][1])
'''zipToPopRdd.saveAsTextFile("zipToSchoolDistrictPop")
from fileinput import input
from glob import glob
open("zipToSchoolDistrictPop.csv","wb").write(''.join(sorted(input(glob("zipToSchoolDistrictPop/part-0000*")))))'''
print popRdd.take(1)
print cityToZipRdd.take(1)
print zipToPopRdd.take(1)


