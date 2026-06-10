package com.streamflix.Estandarizacion

import org.apache.spark.sql.SparkSession


object Modulo2 {


  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    val movieReader = new MovieReader()
    val moviesDF = movieReader.read2(spark, "src/main/resources/data/movies_metadata.csv")

    val movieCleaner = new MovieCleaner()

    movieCleaner.nulosDuplicados(moviesDF)

    val dfPrecioLimpio = movieCleaner.limpiarPrecio(moviesDF)

    val dfFinal = movieCleaner.genresNulos(dfPrecioLimpio)

    dfFinal.printSchema()
    dfFinal.show(5)
  }

}
