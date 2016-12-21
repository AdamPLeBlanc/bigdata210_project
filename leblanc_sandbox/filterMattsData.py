#!/usr/bin/env python
import os
from pyspark import SparkConf, SparkContext


zillowDataPath = r"../data/Zip_Zhvi_AllHomes_Historical.csv"
zipCodeDataPath = r"../data/zip_code_database.csv"
schoolDistrictDataPath = r"../data/WAOFM_-_SAEP_-_School_District_Population_Estimates__2000-2016.csv.tweeked"
demoDataPath = r"../data/14zpallnoagi.csv"

#create a spark context
sc = SparkContext()

#load zipcode data and just save primary city and zip code . column 3 & 0
zipRdd = sc.textFile(zipCodeDataPath).map(lambda line : line.split(',')).map(lambda row : [row[3],row[0]])

#load school district population data, stripping "school district" from the name and saving the numeric & percent change from 2010-2016
sdRdd = sc.textFile(schoolDistrictDataPath).map(lambda line : line.split(':')).map(lambda row : [row[0].replace(" School District",''),row[-5:-1]])

#load housing data and grab city and size rank
housingRdd = sc.textFile(zillowDataPath).map(lambda line : line.split(',')).filter(lambda row : row[3] == "WA").map(lambda row : [row[2],row[6]])

#load demographic data
demoRdd = sc.textFile(demoDataPath).map(lambda line : line.split(',')).map(lambda row : [row[2],row[14]])

finalRdd = zipRdd.join(sdRdd).map(lambda row : [row[0],[row[1][0]] + row[1][1]])
finalRdd = finalRdd.join(housingRdd).map(lambda row : [row[1][0][0],[row[0]]+row[1][0][1:]+[row[1][1]]])
finalRdd = finalRdd.join(demoRdd)

print "sample size: {0}".format(finalRdd.count())
for sample in finalRdd.take(100):
    print sample
'''zipToPopRdd.saveAsTextFile("zipToSchoolDistrictPop")
from fileinput import input
from glob import glob
open("zipToSchoolDistrictPop.csv","wb").write(''.join(sorted(input(glob("zipToSchoolDistrictPop/part-0000*")))))'''



