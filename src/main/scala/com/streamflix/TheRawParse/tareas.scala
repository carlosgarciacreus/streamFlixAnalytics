package com.streamflix.TheRawParse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


class tareas {

  def rrdFiltrado(rdd: RDD[String]) = {
    val rddfilter = rdd.filter(linea => linea.startsWith("[ERROR]") || linea.startsWith("[INFO]"))
    rddfilter
  }

  def rddMapeado(rdd: RDD[String]) = {
    val mapeado = rdd.map { linea =>
      val parts = linea.split(" ", 2)
      val nivel = parts(0)
      val mensaje = parts(1)
      (nivel, mensaje)
    }
    mapeado
  }

  def contar(rddFiltrado: RDD[String]) = {
    val nErrores503 = rddFiltrado.filter(linea => linea.contains("503")).count()
    nErrores503
  }

  def porcentaje(rdd: RDD[String]) = {

    val nErrores = rdd.filter(linea => linea.startsWith("[ERROR]")).count()
    val nLogs = rdd.count()
    val porcentaje = (nErrores.toDouble/nLogs)*100
    porcentaje
  }

}