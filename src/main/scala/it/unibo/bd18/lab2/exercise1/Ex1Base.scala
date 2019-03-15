package it.unibo.bd18.lab2.exercise1

import it.unibo.bd18.lab2.Lab2Base
import org.apache.spark.rdd.RDD

private[exercise1] trait Ex1Base[K, V] extends Lab2Base {

  println("Capra results")
  op(capraRDD).foreach(x => println(s"${x._1} -> ${x._2}"))

  //  println("Divina Commedia results")
  //  op(divinacommediaRDD).foreach(x => println(s"${x._1} -> ${x._2}"))

  protected[this] def op(rdd: RDD[String]): Map[K, V]

}
