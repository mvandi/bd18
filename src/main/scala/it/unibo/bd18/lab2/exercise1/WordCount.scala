package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.util.SparkApp
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordCount extends SparkApp {

  override protected[this] val conf = new SparkConf().setAppName("WordCount App")

  val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

  println(s"Capra result: ${op(capraRDD)}")
//  println(s"Divina Commedia result: ${op(divinacommediaRDD)}")

  def op(rdd: RDD[String]): Map[String, Int] = rdd
    .flatMap(_.split("[^\\w']").toSeq)
    .groupBy(identity).map {
      case (k, v) => k -> v.size
    }
    .collect()
    .toMap

}
