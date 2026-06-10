package com.streamflix.TheRawParse

import org.apache.spark.sql.SparkSession
import com.streamflix.Config


object Modulo1 {

  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    val rdd = sc.textFile(Config.SERVER_LOGS_PATH)

    val tareas = new tareas()
    val rddFiltrado = tareas.rddFiltrado(rdd)

    val lineasDescartadas = rdd.count() - rddFiltrado.count()
    println(s"Líneas descartadas: $lineasDescartadas")

    val rddMapeado = tareas.rddMapeado(rddFiltrado)
    val codigoCantidad = tareas.contarPorNivel(rddMapeado)

    codigoCantidad.coalesce(1).saveAsTextFile(Config.OUTPUT_PATH)
  }
  }