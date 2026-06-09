package com.streamflix

import org.apache.spark.sql.SparkSession
import com.streamflix.TheRawParse.tareas
import com.streamflix.Estandarizacion.MovieReader
import com.streamflix.Estandarizacion.MovieCleaner

object Main extends App {

  // Inicializar SparkSession
  val spark = SparkSession.builder()
    .appName("StreamFlixAnalytics")
    .master("local[*]")
    .getOrCreate()

  // Obtener SparkContext
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val rdd= sc.textFile("src/main/resources/data/server_logs.txt")

  val tareas = new tareas()
  val rddFiltrado = tareas.rddFiltrado(rdd)

  val lineasDescartadas = rdd.count() - rddFiltrado.count()
  println(s"Líneas descartadas: $lineasDescartadas")

  val rddMapeado = tareas.rddMapeado(rddFiltrado)
  val codigoCantidad = tareas.contarPorNivel(rddMapeado)
  codigoCantidad.saveAsTextFile("output/error_counts")


  // MÓDULO 2

  val movieReader = new MovieReader()
  val moviesDF = movieReader.read2(spark, "src/main/resources/data/movies_metadata.csv")

  val movieCleaner = new MovieCleaner()

  val dfPrecioLimpio = movieCleaner.limpiarPrecio(moviesDF)

  movieCleaner.nulosDuplicados(dfPrecioLimpio)

  val dfGenresNulos = movieCleaner.genresNulos(dfPrecioLimpio)
  dfGenresNulos.printSchema()
  dfGenresNulos.show(5)

  spark.stop()
}
