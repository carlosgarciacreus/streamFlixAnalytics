package com.streamflix.Estandarizacion
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class MovieSchema {
  val customSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("genres", StringType, nullable = true),
    StructField("subscription_price", StringType, nullable = true),
    StructField("release_date", StringType, nullable = false),
    StructField("country", StringType, nullable = true)

  ))

}
