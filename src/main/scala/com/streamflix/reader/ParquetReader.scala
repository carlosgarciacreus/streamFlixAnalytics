package com.streamflix.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetReader extends FileReader {
  override def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .parquet(path)
  }
}
