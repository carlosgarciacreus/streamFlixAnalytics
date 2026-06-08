package com.streamflix.TheRawParse

import org.apache.spark.SparkContext

class logReader {

  def read(sc: SparkContext, path: String) = {
    val rdd = sc.textFile(path)
    rdd
  }

}