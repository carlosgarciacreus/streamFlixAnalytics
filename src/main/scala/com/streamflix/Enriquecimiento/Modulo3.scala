package com.streamflix.Enriquecimiento

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.streamflix.TheRawParse.tareas
import com.streamflix.Config
import com.streamflix.Estandarizacion.MovieReader

object Modulo3 {

  def run(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    val tareas = new tareas()
    val rdd = sc.textFile(Config.SERVER_LOGS_PATH)
    val rddFiltrado = tareas.rddFiltrado(rdd).filter(_.startsWith("[INFO]"))

    val logJoin = new logJoin()

    val rddExtraido = logJoin.extraccion(rddFiltrado)
    val logsDF = logJoin.toDataFrame(spark, rddExtraido)


    val movieReader = new MovieReader()
    val moviesDF = movieReader.read2(spark, Config.MOVIES_PATH)
    val enrichedDF = logJoin.broadcastJoin(logsDF, moviesDF)

    enrichedDF.show(10)


    val genreEx = new genreEx()
    val resultado = genreEx.explotar(enrichedDF)
    resultado.orderBy(col("total_hours").desc).show()

    logsDF.show()


  }
}
