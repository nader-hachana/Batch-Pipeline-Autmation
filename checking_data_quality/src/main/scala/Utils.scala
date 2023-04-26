package com.cognira.checking

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, LogManager}
import com.cognira.checking.exceptions.{CompletenessException, IncorrectnessException}


object Utils
{
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)

    // Missing values check:
    def completenessCheck(spark: SparkSession, df: DataFrame, table: String, keyspace: String): Unit  = {
        logger.info("*** APPLYING MISSING VALUES CHECK ***\n")
        import spark.implicits._
        val columnsToCheck = df.columns.filterNot(column => column == "promo_cat" || column == "promo_disc")
        var missingColumns = ListBuffer[(String, Long)]()
        for (column <- columnsToCheck) {
            val missingCount = df.filter(col(column).isNull || isnan(col(column)) || col(column)==="" || col(column).contains("NULL") || col(column).contains("null"))
                                 .count()
            if (missingCount > 0) {missingColumns.prepend((column, missingCount))}
        }
        if (missingColumns.isEmpty) {println(s"Data Completeness validated for table $table in keyspace $keyspace!\nAll columns contain 0 missing values.\n")} 
        else {
            logger.error("*** AN ERROR OCCURED ***\n") 
            try{
                val missingColumnsMsg = missingColumns.map {case (column, count) => s"Column $column has $count missing values"}.mkString("\n");
                val missingColumnsDF = missingColumns.toDF("column_name", "missing_values");
                missingColumnsDF.show() 
                throw new CompletenessException(s"Data completeness check failed for table $table in keyspace $keyspace:\n$missingColumnsMsg"); 
            }
            catch{
                case e: CompletenessException => {
                    println(s"${e.getMessage}\n"); 
                    spark.stop();
                    System.exit(1);
                }
            }
        }   
    }

    // Incorrect values check:
    def incorrectnessCheck(spark: SparkSession, df: DataFrame, table: String, keyspace: String): Unit = {
        logger.info("*** APPLYING INCORRECT VALUES CHECK ***\n")
        val invalidPromoCatCount = 0
        val invalidPromoDiscCount = 0
        if (df.columns.contains("promo_cat") && df.columns.contains("promo_disc")) {
            val invalidPromoCatCount = df.filter(!(col("promo_cat").isin(0, 1, 2, 3, 4, 5) || col("promo_cat") === "nan")).count()
            val invalidPromoDiscCount = df.filter(!(col("promo_disc").rlike("^[0-9]+(\\.[0-9]+)?$") || col("promo_disc") === "nan")).count()
        }
        if (invalidPromoCatCount == 0 && invalidPromoDiscCount == 0) {
            println("Data Incorrectness validated for table $table in keyspace $keyspace!\nNo incorrect values found.\n")
        }
        else {
            logger.error("*** AN ERROR OCCURED ***\n")
            try{
                if (invalidPromoCatCount > 0) {
                    throw new IncorrectnessException(s"${invalidPromoCatCount} rows have invalid values in the promo_cat column for table $table in keyspace $keyspace.")
              }
            }
            catch{
                case e: IncorrectnessException => println(s"${e.getMessage}");
            }
            try{
                if (invalidPromoDiscCount > 0) {
                    throw new IncorrectnessException(s"${invalidPromoDiscCount} rows have invalid values in the promo_disc column for table $table in keyspace $keyspace.")
              }
            } 
            catch{
                case e: IncorrectnessException => println(s"${e.getMessage}");
            }
            spark.stop();
            System.exit(1)
        }  
    }

    def qualityCheck(spark: SparkSession, configFile: String): Unit = {
        val configDF= spark.read
                           .option("header", "true")
                           .option("delimiter","|")
                           .csv(configFile)

        configDF.collect().foreach { row =>
            val keyspace: String = row.getAs[String]("keyspace")
            val table: String = row.getAs[String]("table")

            logger.info(s"*** READING DATA from TABLE $table IN KEYSPACE $keyspace ***")
            val loaded_data = spark.read
                                   .format("org.apache.spark.sql.cassandra")
                                   .options(Map("table" -> table, "keyspace" -> keyspace))
                                   .load()

            val missingValuesDF = completenessCheck(spark, loaded_data, table, keyspace)
            val incorrectValue = incorrectnessCheck(spark, loaded_data, table, keyspace)
        }
    }
}