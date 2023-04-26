package com.cognira.loadingRawData

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import org.apache.log4j.{Level, LogManager}

object Utils
{   
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)
     
    def loadToDb(spark: SparkSession, configFile: String): Unit = {
        // Read the CSV configuration file into a DataFrame
        val configDF= spark.read
                           .option("header", "true")
                           .option("delimiter","|")
                           .csv(configFile)

        configDF.collect().foreach { row =>
            // Extract the configuration parameters from the DataFrame
            val path: String = row.getAs[String]("path")
            val fileName: String = row.getAs[String]("fileName")
            val columnMappingFilePath: String = row.getAs[String]("columnMappingFilePath")
            val header: String = row.getAs[String]("header")
            val writeMode: String = row.getAs[String]("writemode")
            val keyspace: String = row.getAs[String]("keyspace")
            val table: String = row.getAs[String]("table")

            val schemaString = Source.fromFile(columnMappingFilePath).mkString
            val schemaLines = schemaString.replaceAll("\r", "").split("\n")
            val schemaMap = collection.mutable.LinkedHashMap[String, String]()

            for (line <- schemaLines) { 
                val parts = line.split("=")
                schemaMap += (parts(0) -> parts(1)) 
            }
            //schemaMap.foreach { case (key, value) => println(s"$key -> $value") }

            // Read the CSV file using the specified configuration with a Generic Schema
            val df = spark.read
                          .option("header", header)
                          .option("inferSchema", "true")
                          .parquet(s"$path/$fileName")
                          .toDF(schemaMap.keys.toSeq: _*)  
            //df.show()              
            val newColumns = df.columns.map(column => schemaMap.getOrElse(column, column)) 
            val new_df = df.toDF(newColumns: _*)
            //new_df.show()

            // Load dataframes to Cassandra databse
            logger.info(s"*** LOADING RAW DATA TO TABLE $table IN KEYSPACE $keyspace ***")
            new_df.write
              .format("org.apache.spark.sql.cassandra")
              .options(Map("table" -> table, "keyspace" -> keyspace))
              .mode(writeMode)
              .save()
            new_df.show(2)
            // val loaded_data = spark.read
            //                        .format("org.apache.spark.sql.cassandra")
            //                        .options(Map("table" -> table, "keyspace" -> keyspace))
            //                        .load()
            // loaded_data.show(2)
        }
    }    
}