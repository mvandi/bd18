package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base.implicits.RichString
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object InvertedIndex extends Ex1Base[String, Iterable[Long]] {

  override protected[this] val conf = new SparkConf().setAppName("InvertedIndex App")

  override protected[this] def op(rdd: RDD[String]): Map[String, Iterable[Long]] = rdd
    .zipWithIndex
    .flatMap {
      case (l, idx) => l.tokenize.map(_.toLowerCase).map((_, idx))
    }
    .groupBy(_._1)
    .map {
      case (w, it) => (w, it.map(_._2))
    }
    .collect
    .toMap

}
