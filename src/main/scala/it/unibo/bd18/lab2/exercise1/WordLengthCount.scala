package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base.implicits.RichString
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordLengthCount extends Ex1Base[Int, Int] {

  override protected[this] val conf = new SparkConf().setAppName("WordLengthCount App")

  override protected[this] def op(rdd: RDD[String]): Map[Int, Int] = rdd
    .flatMap(_.tokenize)
    .map(_.length)
    .groupBy(identity).map {
      case (k, v) => k -> v.size
    }
    .collect
    .toMap

}
