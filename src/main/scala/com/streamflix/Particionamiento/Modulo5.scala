package com.streamflix.Particionamiento

import com.streamflix.Analisis.Maraton
import com.streamflix.Config
import org.apache.spark.sql.SparkSession
import com.streamflix.Enriquecimiento.logJoin
import com.streamflix.Estandarizacion.MovieReader
import com.streamflix.TheRawParse.tareas

object Modulo5 {

  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    val datascience = new DataScience()
    val movieReader = new MovieReader()
    val moviesDF = movieReader.read2(spark, Config.MOVIES_PATH)

    val rdd = sc.textFile(Config.SERVER_LOGS_PATH)
    val tareas = new tareas()

    val logJoin = new logJoin()
    val rddFiltrado = tareas.rddFiltrado(rdd).filter(_.startsWith("[INFO]"))
    val rddExtraido3 = logJoin.extraccion(rddFiltrado)
    val logsDF = logJoin.toDataFrame(spark, rddExtraido3)

    val enrichedDF = logJoin.broadcastJoin(logsDF, moviesDF)

    datascience.guardar(enrichedDF)
  }

}
