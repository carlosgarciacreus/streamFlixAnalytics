package com.streamflix

import org.apache.spark.sql.SparkSession
import com.streamflix.TheRawParse.tareas

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


  spark.stop()
}
