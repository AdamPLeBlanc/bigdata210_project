package org.uw.income.test

import org.uw.income.Keys
import org.uw.income.AgiValues

// Testing

import org.junit.{Test, After, Before}
import org.scalatest.junit.JUnitSuite

// Spark

import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class IncomeTest extends JUnitSuite {
    val INCOME_DATA_FILE = "./14zpallnoagi.csv"
    val SCHOOL_DISTR_POP_FILE = "./WAOFM_-_SAEP_-_School_District_Population_Estimates__2000-2016.csv"
    val ZIP_TO_CITY_FILE = "./zip_code_database.csv"

    @Before def setUp() {
        println("IncomeTest:Setup")
    }

    @After def tearDown() {
        println("IncomeTest:Teardown")
    }

    @Test def runIncomeTest(): Unit = {
        val sparkContext = TestUtils.getSparkContext()
        val sqlContext = SQLContext.getOrCreate(sparkContext)
        
        val schoolPopDf = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(SCHOOL_DISTR_POP_FILE)
            
        val cleanedSchoolPopDf = schoolPopDf.selectExpr(
            "regexp_replace(Unified_School_District_Name, ' School District', '') as CITY",
            "Percent_Change_in_Population_2000_to_2010 as CHANGE_PERCENT_2000_2010",
            "Percent_Change_in_Population_2010_to_2016 as CHANGE_PERCENT_2010_2016")
        cleanedSchoolPopDf.registerTempTable("school_pop")

        val zipToCityDf = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(ZIP_TO_CITY_FILE)
        zipToCityDf.registerTempTable("zip_codes")
            
        val incomeDf = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(INCOME_DATA_FILE)          
        incomeDf.registerTempTable("incomes")
        
        val selectedData = sqlContext.sql(
            s"SELECT incomes.STATE, zip_codes.zip, zip_codes.primary_city, school_pop.CHANGE_PERCENT_2010_2016, round(incomes.${Keys.KEY_SALARIES_AND_WAGES}/incomes.${Keys.KEY_NUM_RETURNS_WITH_SALARIES_AND_WAGES},0) AS AVG_SALARY_IN_K FROM incomes INNER JOIN zip_codes ON zip_codes.zip = incomes.ZIPCODE INNER JOIN school_pop ON school_pop.CITY = zip_codes.primary_city WHERE incomes.STATE = 'WA'")
        
        val sortedData = selectedData.orderBy(org.apache.spark.sql.functions.col("AVG_SALARY_IN_K").desc)
        sortedData.show(100)
        sparkContext.stop()
    }
}

/*
//s"SELECT ${Keys.KEY_STATE}, ${Keys.KEY_5DIGIT_ZIP_CODE}, concat(cast(round(${Keys.KEY_SALARIES_AND_WAGES}/${Keys.KEY_NUM_RETURNS_WITH_SALARIES_AND_WAGES},0) AS VARCHAR(64)), ' K') AS WAGES FROM incomes WHERE ${Keys.KEY_5DIGIT_ZIP_CODE} = '98008'")

        val selectedData: DataFrame = dataframe.selectExpr(
            Keys.KEY_STATE,
            Keys.KEY_5DIGIT_ZIP_CODE,
            s"concat(cast(round(${Keys.KEY_SALARIES_AND_WAGES}/${Keys.KEY_NUM_RETURNS_WITH_SALARIES_AND_WAGES},0) as varchar(64)), ' K')")
            .filter(s"${Keys.KEY_5DIGIT_ZIP_CODE} = '98008'")
            .filter(s"${Keys.KEY_SIZE_OF_ADJUSTED_GROSS_INCOME} = '${AgiValues.AGI_NONE}'")
            
*/