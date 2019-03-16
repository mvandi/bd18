package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base
import org.apache.spark.rdd.RDD

private[exercise1] trait Ex1Base[K, V] extends Lab2Base {

  printResults("Capra", op(capraRDD))
//  println()
//  printResults("Divina Commedia", op(divinacommediaRDD))

  protected[this] def op(rdd: RDD[String]): Map[K, V]

  private def printResults(label: String, results: Map[_, _]): Unit = {
    println(s"$label results")
    results.foreach(x => println(s"${x._1} -> ${x._2}"))
  }

}
