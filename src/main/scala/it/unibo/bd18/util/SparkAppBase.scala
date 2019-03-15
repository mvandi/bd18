package it.unibo.bd18.util

import org.apache.spark.{SparkConf, SparkContext}

private[util] trait SparkAppBase extends App {

  protected[this] val conf: SparkConf = new SparkConf()

  protected[this] def sc: SparkContext

  object implicits {
    println("Implicits")
    implicit lazy val _sc: SparkContext = sc
  }

}
