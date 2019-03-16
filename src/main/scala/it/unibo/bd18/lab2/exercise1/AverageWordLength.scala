package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base.implicits.RichString
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object AverageWordLength extends Ex1Base[String, Double] {

  override protected[this] val conf = new SparkConf().setAppName("AverageWordLength App")

  override protected[this] def op(rdd: RDD[String]): Map[String, Double] = rdd
    .flatMap(_.tokenize)
    .map(_.toLowerCase)
    .map(x => (x.firstLetter, x.length))
    .groupBy(_._1)
    .map {
      case (k, v) => k -> (v.map(_._2).sum / v.size.toDouble)
    }
    .collect()
    .toMap

}
