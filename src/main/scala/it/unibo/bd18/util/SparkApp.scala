package it.unibo.bd18.util

import org.apache.spark.SparkContext

trait SparkApp extends SparkAppBase {

  protected[this] override final lazy val sc: SparkContext = {
    require(conf != null, "conf cannot be null")
    SparkContext.getOrCreate(conf)
  }

}
