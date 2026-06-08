package com.streamflix.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class TxtReader extends FileReader {
  override def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .text(path)
  }
}
