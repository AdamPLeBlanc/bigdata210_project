#!/usr/bin/env python
import os
from pyspark import SparkConf, SparkContext

def blah(row):
    row[0] = row[0].replace(" School District",'')
    return row


dataPath = r"../data/Zip_Zhvi_AllHomes_Historical.csv"

#load school district population data
parseCsv = lambda line : line.split(',')
sc = SparkContext()
origRdd = sc.textFile(dataPath).map(parseCsv).filter(lambda line : line[3] == "WA").map(lambda line : line[:6])
print "sample size: {0}".format(origRdd.count())
for sample in origRdd.take(100):
    print sample
'''zipToPopRdd.saveAsTextFile("zipToSchoolDistrictPop")
from fileinput import input
from glob import glob
open("zipToSchoolDistrictPop.csv","wb").write(''.join(sorted(input(glob("zipToSchoolDistrictPop/part-0000*")))))'''



