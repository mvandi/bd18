package it.unibo.bd18.lab2.exercise1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object AverageWordLength extends Ex1Base[String, Double] {

  override protected[this] val conf = new SparkConf().setAppName("AverageWordLength App")

  override protected[this] def op(rdd: RDD[String]): Map[String, Double] = rdd
    .flatMap(_.split("\\w_").toSeq)
    .map(x => (x.substring(0, 1), x.length))
    .groupBy(_._1)
    .map {
      case (k, v) => k -> (v.map(_._2).sum / v.size.toDouble)
    }
    .collect
    .toMap

}
