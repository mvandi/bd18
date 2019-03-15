package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.util.SparkApp
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordLengthCount extends SparkApp {

  override protected[this] val conf = new SparkConf().setAppName("CountWordOccurrences App")

  val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

  def countWordLength(rdd: RDD[String]): Map[Int, Int] = rdd
    .flatMap(_.split("\\s+").toSeq)
    .map(_.length)
    .groupBy(identity).map {
    case (k, v) => k -> v.size
  }.collect().toMap

}
