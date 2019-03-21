package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base.implicits._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object InvertedIndex extends Ex1Base[String, Iterable[Long]] {

  import it.unibo.bd18.util.implicits.RichPairRDD

  override protected[this] val conf = new SparkConf().setAppName("InvertedIndex App")

  override protected[this] def op(rdd: RDD[String]): Map[String, Iterable[Long]] = rdd
    .zipWithIndex()
    .flatMapKeys(_.tokenize.map(_.toLowerCase))
    .groupByKey()
    .collect()
    .toMap

}
