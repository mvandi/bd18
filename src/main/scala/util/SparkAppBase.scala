package util

import org.apache.spark.SparkConf

private[util] trait SparkAppBase extends App {

  protected[this] val conf: SparkConf

}
