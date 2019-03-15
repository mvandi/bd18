package it.unibo.bd18.lab2

import it.unibo.bd18.util.{Logging, SparkApp}
import org.apache.spark.SparkConf

object Exercise0 extends SparkApp with Logging {

  override protected[this] val conf = new SparkConf().setAppName("Exercise1 App")

  val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

  capraRDD.collect { case x: Any => log.info(x) }
  divinacommediaRDD.collect { case x: Any => log.info(x) }

  log.info(s"Capra line count: ${capraRDD.count()}")
  log.info(s"Divina commedia line count: ${divinacommediaRDD.count()}")

  capraRDD map(_.split("\\s+")) foreach(println(_))
  divinacommediaRDD map(_.split("\\s+")) foreach(println(_))

}
