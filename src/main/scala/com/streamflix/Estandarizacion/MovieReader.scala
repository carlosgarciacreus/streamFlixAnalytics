package com.streamflix.Estandarizacion

import com.streamflix.reader.CsvReader
import com.streamflix.Estandarizacion.MovieSchema
import org.apache.spark.sql.SparkSession

class MovieReader {

  def read2 (spark: SparkSession, path: String) = {

    val movieSchema = new MovieSchema()
    val csvReader = new CsvReader()
    csvReader.readWithSchema(spark, path, movieSchema.customSchema)
  }

}
