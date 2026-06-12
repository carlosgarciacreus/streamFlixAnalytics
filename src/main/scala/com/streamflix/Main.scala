package com.streamflix

import org.apache.spark.sql.SparkSession
import com.streamflix.TheRawParse.Modulo1
import com.streamflix.Estandarizacion.Modulo2
import com.streamflix.Enriquecimiento.Modulo3
import com.streamflix.Analisis.Modulo4
import com.streamflix.Particionamiento.Modulo5


object Main extends App {
  System.setProperty("hadoop.home.dir", "C:\\hadoop")

  val spark = SparkSession.builder()
    .appName("StreamFlixAnalytics")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  args(0) match {
    case "1" => Modulo1.run(spark)
    case "2" => Modulo2.run(spark)
    case "3" => Modulo3.run(spark)
    case "4" => Modulo4.run(spark)
    case "5" => Modulo5.run(spark)
    case _   => println("Módulo no encontrado")
  }
}
