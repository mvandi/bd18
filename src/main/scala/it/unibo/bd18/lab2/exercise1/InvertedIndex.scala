package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.util.SparkApp
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object InvertedIndex extends SparkApp {

  override protected[this] val conf = new SparkConf().setAppName("CountWordOccurrences App")

  val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

  def invertedIndex(rdd: RDD[String]): Unit = rdd
    .zipWithIndex()
    .flatMap {
      case (l, idx) => l.split("\\s+").map((_, idx)).toSeq
    }
    .groupBy(_._1)
    .map {
      case (w, it) => (w, it.map(_._2))
    }
    .collect().toMap

}
