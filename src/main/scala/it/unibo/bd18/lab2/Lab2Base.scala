package it.unibo.bd18.lab2

import it.unibo.bd18.util.SparkApp

private[lab2] trait Lab2Base extends SparkApp {

  final val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  final val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

}
