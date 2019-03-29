package it.unibo.bd18.lab2

import Lab2Base.implicits.RichString
import org.apache.spark.SparkConf

object Exercise0 extends Lab2Base {

  override protected[this] val conf = new SparkConf().setAppName("Exercise0 App")

  // Show lines in a file
  capraRDD.collect { case x: Any => println(x) }
  divinacommediaRDD.collect { case x: Any => println(x) }

  // Count number of lines in a file
  println(s"Capra line count: ${capraRDD.count}")
  println(s"Divina Commedia line count: ${divinacommediaRDD.count}")

  // Split phrases into words
  capraRDD map(_.tokenize) foreach(println(_))
  divinacommediaRDD map(_.tokenize) foreach(println(_))

}
