package it.unibo.bd18.lab2

import it.unibo.bd18.util.Logging
import org.apache.spark.SparkConf

object Exercise0 extends Lab2Base with Logging {

  override protected[this] val conf = new SparkConf().setAppName("Exercise1 App")

  capraRDD.collect { case x: Any => log.info(x) }
  divinacommediaRDD.collect { case x: Any => log.info(x) }

  log.info(s"Capra line count: ${capraRDD.count()}")
  log.info(s"Divina commedia line count: ${divinacommediaRDD.count()}")

  capraRDD map(_.split("\\w_")) foreach(println(_))
  divinacommediaRDD map(_.split("\\W_")) foreach(println(_))

}
