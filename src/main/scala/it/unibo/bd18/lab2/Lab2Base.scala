package it.unibo.bd18.lab2

import it.unibo.bd18.app.SparkApp

private[lab2] trait Lab2Base extends SparkApp {

  final lazy val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra").cache
  final lazy val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

}

private[lab2] object Lab2Base {

  private val SPLIT_REGEX = "[\\W_]+"

  object implicits {

    implicit class RichString(private val s: String) {
      def tokenize: Seq[String] = s.trim().split(SPLIT_REGEX).toSeq.filter(_.nonEmpty)

      def firstLetter: String = s.substring(0, 1)
    }

  }

}
