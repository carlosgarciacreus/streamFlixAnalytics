package com.streamflix.Analisis

import com.streamflix.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Modulo4 {

  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    val rdd = sc.textFile(Config.SERVER_LOGS_PATH)

    val maraton = new Maraton()

    val rddExtraido = maraton.extraccion(rdd)

    val dfExtraido = maraton.toDataFrame(spark, rddExtraido)

    val ventana = maraton.windowSpec()

    val lagAplicado = maraton.aplicarLag(dfExtraido, ventana)

    val diferencia = maraton.diferencia(lagAplicado)

    val is_binge = maraton.is_binge(diferencia)

    val mostrar = maraton.mostrar(is_binge)

    mostrar.show()
  }
}
