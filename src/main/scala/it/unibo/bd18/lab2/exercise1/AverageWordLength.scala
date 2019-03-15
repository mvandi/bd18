package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.util.SparkApp
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object AverageWordLength extends SparkApp {

  override protected[this] val conf = new SparkConf().setAppName("AverageWordLength App")

  val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

  println(s"Capra result: ${op(capraRDD)}")
//  println(s"Divina Commedia result: ${op(divinacommediaRDD)}")

  def op(rdd: RDD[String]): Map[String, Double] = rdd
    .flatMap(_.split("\\s+").toSeq)
    .map(x => (x.substring(0, 1), x.length))
    .groupBy(_._1)
    .map {
      case (k, v) => k -> (v.map(_._2).sum / v.size.toDouble)
    }
    .collect()
    .toMap

}
