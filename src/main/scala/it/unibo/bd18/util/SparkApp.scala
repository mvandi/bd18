package it.unibo.bd18.util

import org.apache.spark.SparkContext

trait SparkApp extends SparkAppBase {

  protected[this] final lazy val sc: SparkContext = {
    require(conf != null, "conf cannot be null")
    new SparkContext(conf)
  }

  object Implicits {
    implicit lazy val _sc: SparkContext = sc
  }

}
