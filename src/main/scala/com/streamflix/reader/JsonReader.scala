package com.streamflix.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonReader extends FileReader {
  override def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("multiline", "true")
      .json(path)
  }
}