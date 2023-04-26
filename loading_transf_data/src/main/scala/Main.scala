package com.cognira.loadingTransfData

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, LogManager}
import com.cognira.loadingTransfData.Utils.{loadingData}


object Main
{
    def main(args: Array[String]): Unit=
    {
        val logger = LogManager.getRootLogger
        logger.setLevel(Level.INFO)

        logger.info("*** PREPARING SPARK SESSION ***")
        // creating the spark configuration
        val conf: SparkConf = new SparkConf(true)
            .set("spark.cassandra.connection.host", "cassandra")
            .set("spark.cassandra.connection.port", "9042")
            .set("spark.cassandra.auth.username", sys.env.getOrElse("DB_USER", "cassandra"))
            .set("spark.cassandra.auth.password", sys.env.getOrElse("DB_PASS", "cassandra"))

        // creating the spark session
        val spark =  SparkSession
            .builder()
            .master("local[*]")
            .config(conf)
            .getOrCreate()

        // limiting the logs to WARN and ERROR
        spark.sparkContext.setLogLevel("ERROR")

        // loading data from parquet files to Cassandra
        val configFile= "/opt/schema_config.csv"
        val loading = loadingData(spark, conf, configFile)
        
        logger.info("*** STOPPING SPARK SESSION ***")
        spark.stop()
    }
}