package com.streamflix.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvReader extends FileReader {
  override def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
}