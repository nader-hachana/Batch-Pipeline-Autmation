package com.cognira.processing

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import com.cognira.processing.Utils.readDfFromDb
import com.cognira.processing.Metrics.{weeklySales, totalSales, baseline, lift, absolute_diff, sales_error, profit_error, price_transformation}
import org.apache.log4j.{Level, LogManager}

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
        //spark.sparkContext.setLogLevel("ERROR")
        spark.catalog.clearCache()
        
         // Metrics computation
        logger.info("*** APPLYING METRICS CALCULATION ***")

        // Reading transaction logs data from the database
        val datagen = readDfFromDb(spark, "sales", "datagen_0_v1")
        val t_data = datagen.drop("customer_id").withColumn("units_sold",lit(1)).withColumn("tenant_id",lit(0))
        //t_data.show(2)

        logger.info("*** METRIC: TOTAL SALES ***")
        val total_sales_df = totalSales(spark, t_data)
        total_sales_df.show(2)
        total_sales_df.write.mode("overwrite").parquet("/opt/transformations/total_sales.parq")

        logger.info("*** METRIC: WEEKLY SALES ***")
        val weekly_sales_df = weeklySales(spark, t_data)
        weekly_sales_df.show(2)
        weekly_sales_df.write.mode("overwrite").parquet("/opt/transformations/weekly_sales.parq")

        logger.info("*** METRIC: BASELINE ***")
        val baseline_df = baseline(spark, t_data)
        baseline_df.show(2)
        baseline_df.write.mode("overwrite").parquet("/opt/transformations/baseline.parq")

        logger.info("*** METRIC: LIFT ***")
        val lift_df = lift(spark, weekly_sales_df, baseline_df)
        lift_df.show(2)
        lift_df.write.mode("overwrite").parquet("/opt/transformations/lift.parq")

        val forecasts = readDfFromDb(spark, "sales", "forecasts_v1")
                             .withColumn("tenant_id",lit(0))
                             .select("tenant_id","purch_week","sku","store_id","prediction")

        val forecast_df = weekly_sales_df.join(forecasts,Seq("tenant_id","purch_week","store_id","sku"),"left_outer")
                                         
        logger.info("*** METRIC: SALES ERROR ***")
        val sales_error_df = sales_error(spark, forecast_df)
        sales_error_df.show(2)
        sales_error_df.write.mode("overwrite").parquet("/opt/transformations/sales_error.parq")
        
        logger.info("*** METRIC: ABSOLUTE DIFFERENCE ***")
        val absolute_diff_df = absolute_diff(spark, forecast_df)
        absolute_diff_df.show(2)
        absolute_diff_df.write.mode("overwrite").parquet("/opt/transformations/absolute_diff.parq")

        val products = readDfFromDb(spark, "sales", "products_v1")
                            .select("sku","prod_base_price")

        logger.info("*** METRIC: PROFIT ERROR ***")
        val profit_error_df = profit_error(spark, forecast_df, products)
        profit_error_df.show(2)
        profit_error_df.write.mode("overwrite").parquet("/opt/transformations/profit_error.parq")

        val calendar =  readDfFromDb(spark, "sales", "calendar_v1")
                             .withColumn("calendar_day", to_date(col("calendar_day"), "MM-dd-yy"))
                             .orderBy("calendar_day")
                             .withColumn("purch_week",when(year(col("calendar_day")) === 2018, col("week_of_year") - 1).when(year(col("calendar_day")) === 2019, col("week_of_year") + 51).otherwise(col("week_of_year") + 103))
                             .select("calendar_day","purch_week")

        logger.info("*** METRIC: PRICE TRANSFORMATION ***")
        val price_transf = price_transformation(spark, t_data, products, calendar)
        price_transf.show(2, truncate=false)
        price_transf.write.mode("overwrite").parquet("/opt/transformations/price_transf.parq")

        logger.info("*** STOPPING SPARK SESSION ***")
        spark.stop()
    }
}