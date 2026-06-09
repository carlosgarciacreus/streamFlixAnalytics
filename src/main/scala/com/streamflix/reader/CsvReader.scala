package com.streamflix.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class CsvReader extends FileReader {
  override def read(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def readWithSchema(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .csv(path)
  }

}
