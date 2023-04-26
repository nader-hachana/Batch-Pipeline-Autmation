package com.cognira.processing

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Metrics
{
    def totalSales(spark: SparkSession, df: DataFrame): DataFrame = {
      df.groupBy("tenant_id","sku","promo_cat","promo_disc")
        .agg(sum("units_sold").alias("total_sales_promo_prod"))
    }

    def weeklySales(spark: SparkSession, df: DataFrame): DataFrame = {
      df.groupBy("tenant_id","purch_week","sku","store_id","promo_cat","promo_disc")
        .agg(sum("units_sold").alias("weekly_sales"))
        
    }

    def baseline(spark: SparkSession, df: DataFrame): DataFrame = {
      df.filter(col("promo_cat") === "nan")
        .groupBy("tenant_id","sku","store_id")
        .agg(expr("percentile(units_sold, 0.5)").as("baseline"))
        
    }

    def lift(spark: SparkSession, df: DataFrame, df2: DataFrame): DataFrame = {
      val weekly_sales_df = df
      val baseline_df = df2
      weekly_sales_df.join(baseline_df,Seq("tenant_id","sku","store_id"),"left_outer")
                      .withColumn("lift_unit", col("weekly_sales") - col("baseline"))
                      .withColumn("lift_percentage", when(isnan(col("promo_cat")), 1.0).otherwise(col("weekly_sales") / col("baseline")))
    }

    def sales_error(spark: SparkSession, df: DataFrame): DataFrame = {
      df.withColumn("error", abs(col("prediction") - col("weekly_sales")) / abs(col("weekly_sales")))
    }

    def absolute_diff(spark: SparkSession, df: DataFrame): DataFrame = {
      df.withColumn("absolute_diff", abs(col("prediction") - col("weekly_sales")))
    }

    def profit_error(spark: SparkSession, df: DataFrame, df2: DataFrame): DataFrame = {
      df.join(df2, Seq("sku"), "left_outer")
        .withColumn("profit_error", abs(col("weekly_sales") * col("prod_base_price") -  col("prediction") * col("prod_base_price")) / abs(col("weekly_sales") * col("prod_base_price")))
    }

    def price_transformation(spark: SparkSession, df: DataFrame, df2: DataFrame, df3: DataFrame): DataFrame = {
      val price = df.drop("promo_cat","units_sold")
                    .join(df2, Seq("sku"), "left_outer")
      val full_price = price.join(df3, Seq("purch_week"), "left_outer")
                            .withColumn("prod_new_price", when(col("promo_disc") =!= "nan", col("prod_base_price") - col("prod_base_price") * col("promo_disc")).otherwise(col("prod_base_price")))
                            .drop("purch_week","prod_base_price")
      val windowSpec = Window.partitionBy("tenant_id", "sku", "store_id").orderBy("calendar_day")

      full_price.withColumn("prev_price", lag("prod_new_price", 1).over(windowSpec))
                .filter(col("prod_new_price") =!= col("prev_price") || col("prev_price").isNull)
                .groupBy("tenant_id", "sku", "store_id")
                .agg(array_sort(collect_list(struct("calendar_day", "prod_new_price"))).alias("price"))
    }
}