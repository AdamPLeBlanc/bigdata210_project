#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

import os
import collections


dropoutRateCsvPath = r"../data/High_School_Dropout_Statistics_by_County_2012-2013_School_Year_5-Year_Cohort_Dropout_Rates.csv"
enrollmentCsvPath = r"../data/K-12_Public_School_Enrollment_by_Grade_Level_October_Enrollment_Report_2009-2013.csv"
schoolDistrictPopCsvPath = r"../data/WAOFM_-_SAEP_-_School_District_Population_Estimates__2000-2016.csv"

#load school district population data
parseCsv = lambda line : line.split(',')
sc = SparkContext()
dropoutRdd = sc.textFile(dropoutRateCsvPath).map(parseCsv)
enrollmentRdd = sc.textFile(enrollmentCsvPath).map(parseCsv)
popRdd = sc.textFile(schoolDistrictPopCsvPath).map(parseCsv)
print dropoutRdd.take(5)
print enrollmentRdd.take(5)
print popRdd.take(5)
print "done"
