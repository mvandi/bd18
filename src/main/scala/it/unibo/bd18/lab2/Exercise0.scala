package it.unibo.bd18.lab2

import it.unibo.bd18.util.SparkApp
import org.apache.spark.SparkConf

object Exercise0 extends SparkApp {

  override protected[this] val conf = new SparkConf().setMaster("...").setAppName("Exercise1 App")

  import Implicits._
  import it.unibo.bd18.util.implicits._

  val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

  val RDDs = Seq(capraRDD, divinacommediaRDD)

  capraRDD.collect { case x: Any => println(x) }
  println()
  divinacommediaRDD.collect { case x: Any => println(x) }

  println(s"Capra line count: ${capraRDD.count()}")
  println(s"Divina commedia line count: ${divinacommediaRDD.count()}")

  capraRDD.map(_.split("\\s+"))

}
