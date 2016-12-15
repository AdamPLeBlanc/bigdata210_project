package org.uw.income.test

// Spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * TestUtils: contains the following function: getSparkContext
 */
object TestUtils {
    val OUTPUT_FILE_DIR = "./output"
    val TEMP_DIR = "./temp/"

    /** getSparkContext: Set up spark with the localhost spark settings and return the context.
      * @return sparkContext on local host with two core and app name delinquent_handler_junit
      */
    def getSparkContext(): SparkContext = {

        // Set up spark.
        val sparkConf = new SparkConf().setAppName("archiver_junit").setMaster("local[1]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.rdd.compress", "true")

        return new SparkContext(sparkConf)
    }
}