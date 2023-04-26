package com.cognira.loadingTransfData

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import com.datastax.spark.connector.cql.CassandraConnector


object Utils
{
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)

    def loadingData(spark: SparkSession, conf: SparkConf, configFile: String): Unit = {
        // Create a Cassandra session
        val cassandraSession = CassandraConnector(conf).openSession()

        // Read the CSV configuration file into a DataFrame
        val configDF= spark.read
                        .option("header", "true")
                        .option("delimiter","|")
                        .csv(configFile)
        try{
            configDF.collect().foreach { row =>
                // Extract the configuration parameters from the DataFrame
                val path: String = row.getAs[String]("path")
                val fileName: String = row.getAs[String]("fileName")
                val writeMode: String = row.getAs[String]("writemode")
                val delimiter: String = row.getAs[String]("delimiter")
                val keyspace: String = row.getAs[String]("keyspace")
                val table: String = row.getAs[String]("table")
                val columns: String = row.getAs[String]("columns")
                val primaryKey: String = row.getAs[String]("primary_key")
                val clusteringKey: String = Option(row.getAs[String]("clustering_key")).getOrElse("")

                // Read the Parquet file using the specified configuration
                val df= spark.read
                            .option("header", "true")
                            .option("delimiter", delimiter)
                            .parquet(s"$path/$fileName")
                
                // Read the Columns and Types 
                val schemaMap = collection.mutable.LinkedHashMap[String, String]()
                val fields = columns.split(";")
                fields.foreach { field =>
                    val nameAndType = field.split(":")
                    schemaMap += (nameAndType(0) -> nameAndType(1)) 
                }

                // Create the Cassandra table using the specified configuration and session
                val clusteringKeyStr = if (clusteringKey.nonEmpty) s",${clusteringKey}" else ""
                cassandraSession.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };")
                cassandraSession.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (${schemaMap.map{ case (colName,colType) => s"$colName $colType"}.mkString(", ")}, PRIMARY KEY (($primaryKey)$clusteringKeyStr));")

                // Write the DataFrame to Cassandra using the specified keyspace, table, and write mode
                logger.info(s"*** LOADING DATA TO TABLE $table IN KEYSPACE $keyspace ***")
                df.selectExpr(schemaMap.keys.toSeq: _*)     // I used selectExpr because I provided a Seq     
                  .write
                  .format("org.apache.spark.sql.cassandra")
                  .options(Map("table" -> table, "keyspace" -> keyspace))
                  .mode(writeMode)
                  .save()
                val loaded_data = spark.read
                                       .format("org.apache.spark.sql.cassandra")
                                       .options(Map("table" -> table, "keyspace" -> keyspace))
                                       .load()
                loaded_data.show(2)
                }
            } 
        finally {
            // Close the Cassandra session when finished
            cassandraSession.close()
        }
    }
}