package it.unibo.bd18.lab2

import it.unibo.bd18.util.SparkApp
import it.unibo.bd18.util.implicits._
import org.apache.spark.SparkConf

object Test extends SparkApp {

  override protected[this] val conf = new SparkConf().setMaster("local[*]").setAppName("Test")

  private val seq: Seq[Int] = Seq(1, 2, 3, 4, 5)

//  import Implicits._
//  println(seq.toRDD)

  sc.parallelize(seq)

}
