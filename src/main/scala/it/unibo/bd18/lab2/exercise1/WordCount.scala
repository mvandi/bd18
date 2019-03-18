package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base.implicits._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordCount extends Ex1Base[String, Int] {

  override protected[this] val conf = new SparkConf().setAppName("WordCount App")

  override protected[this] def op(rdd: RDD[String]): Map[String, Int] = rdd
    .flatMap(_.tokenize)
    .map(_.toLowerCase)
    .groupBy(identity)
    .mapValues(_.size)
    .collect()
    .toMap

}
