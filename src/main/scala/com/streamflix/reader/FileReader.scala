package com.streamflix.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait FileReader {
  def read(spark: SparkSession, path: String): DataFrame
}
