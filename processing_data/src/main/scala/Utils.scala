package com.cognira.processing

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Utils
{
    // Generic Schema:  
    def readDfFromDb(spark: SparkSession,keyspace:String, table: String): DataFrame = {
        val loaded_data = spark.read
                                .format("org.apache.spark.sql.cassandra")
                                .options(Map("table" -> table, "keyspace" -> keyspace))
                                .load()
        return loaded_data
    }  
}