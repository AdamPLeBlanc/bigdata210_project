#!/usr/bin/env python

import collections
import os

from pyspark import SparkConf, SparkContext
from operator import add

airportCsvPath = "/tmp/airports.csv"
flightCsvPath = "/tmp/flights.csv"
carriersCsvPath = "/tmp/carriers.csv"

sc = SparkContext()
flightRdd = sc.textFile(flightCsvPath).map(lambda line : line.split(","))
carrierRdd = sc.textFile(carriersCsvPath).map(lambda line : line.split(","))
carrierRdd.sortByKey()
carrierFlightsRdd = flightRdd.map(lambda line : (line[5],1))

flightStats = sorted(carrierFlightsRdd.reduceByKey(add).collect(),lambda x,y : cmp(y[1],x[1]))
flightStats = map(lambda item : (carrierRdd.lookup(item[0])[0],item[1]),flightStats)
print
print"Top 3 Airlines With Most Flights"
print os.linesep.join(["{0}:  {1}".format(fs[0],fs[1]) for fs in flightStats[:3]])
print
print

airportRdd = sc.textFile(airportCsvPath).map(lambda line : [l.strip('"') for l in line.split(",")])
cityRdd = airportRdd.map(lambda line : (line[0],line[2]))
cityRdd.sortByKey()
cityHM = dict(cityRdd.collect())
flightOrigDestRdd = flightRdd.map(lambda line : (line[12],line[13]))
routeStats = sorted(flightOrigDestRdd.map(lambda line : [(cityHM[line[0]],cityHM[line[1]]),1]).reduceByKey(add).collect(),lambda x,y : cmp(y[1],x[1]))

print "Top 5 busiest routes"
print os.linesep.join(["{0} to {1}:  {2}".format(c[0],c[1],n) for c,n in routeStats[:5]])
print
'''
tmp2 = sorted(tmp.reduceByKey(add).collect(),lambda x,y : cmp(y[1],x[1]))
print
print "Top 5 Routes Between Cities"
print tmp2
print
#print os.linesep.join(["{0} - {1}:  {2}".format(c[0],c[1],n) for
'''
