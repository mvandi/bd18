package it.unibo.bd18.util

import org.apache.spark.SparkContext

trait SparkApp extends SparkAppBase {

  protected[this] override final lazy val sc: SparkContext = SparkContext.getOrCreate(conf)

}
