package it.unibo.bd18.lab2

import it.unibo.bd18.util.SparkApp

private[lab2] trait Lab2Base extends SparkApp {

  final val capraRDD = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt").cache
  final val divinacommediaRDD = sc.textFile("hdfs:/bigdata/dataset/divinacommedia").cache

}

private[lab2] object Lab2Base {

  private val SPLIT_REGEX = "((\\b[^\\s]+\\b)((?<=\\.\\w).)?)"

  object implicits {

    implicit class RichString(private val s: String) {
      def tokenize: Seq[String] = s.split(SPLIT_REGEX).toSeq.filter(_.nonEmpty)

      def firstLetter: String = s.substring(0, 1)
    }

  }

}
