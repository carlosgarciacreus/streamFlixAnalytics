package com.streamflix.Particionamiento

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

class DataScience {


  def guardar(enrichedDF: DataFrame): Unit = {
    val dfcompleto = enrichedDF.withColumn("year", year(col("release_date")))
    dfcompleto.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "country")
      .parquet("src/main/resources/output/analytics_warehouse")
  }

}
