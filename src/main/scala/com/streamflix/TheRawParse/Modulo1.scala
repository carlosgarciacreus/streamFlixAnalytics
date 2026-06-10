package com.streamflix.TheRawParse

import org.apache.spark.sql.SparkSession
import com.streamflix.TheRawParse.tareas
import org.apache.hadoop.fs.{FileSystem, Path}

object Modulo1 {

  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    val rdd = sc.textFile("src/main/resources/data/server_logs.txt")

    val tareas = new tareas()
    val rddFiltrado = tareas.rddFiltrado(rdd)

    val lineasDescartadas = rdd.count() - rddFiltrado.count()
    println(s"Líneas descartadas: $lineasDescartadas")

    val rddMapeado = tareas.rddMapeado(rddFiltrado)
    val codigoCantidad = tareas.contarPorNivel(rddMapeado)

    codigoCantidad.coalesce(1).saveAsTextFile("output/error_counts")
  }
  }