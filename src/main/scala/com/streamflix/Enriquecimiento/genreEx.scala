package com.streamflix.Enriquecimiento
import org.apache.spark.sql.functions.{explode, split, col, sum}
import org.apache.spark.sql.DataFrame


class genreEx {

  def explotar(enrichedDF: DataFrame): DataFrame = {
    val genreMetricsDF = enrichedDF
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .groupBy("genre")
      .agg(sum("duration_watched").alias("total_hours"))
    genreMetricsDF
  }
}
