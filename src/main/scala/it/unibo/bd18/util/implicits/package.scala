package it.unibo.bd18.util

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.reflect.ClassTag

package object implicits {

  implicit class RichSeq[T](private val seq: Seq[T]) {
    def toRDD(parallelism: Int)(implicit sc: SparkContext, ct: ClassTag[T]): RDD[T] = sc.makeRDD(seq, parallelism)

    def toRDD(implicit sc: SparkContext, ct: ClassTag[T]): RDD[T] = toRDD(sc.defaultParallelism)
  }

  implicit class RichSeqWithNewPartitions[T](private val seq: Seq[(T, Seq[String])]) {
    def toRDD(implicit sc: SparkContext, ct: ClassTag[T]): RDD[T] = sc.makeRDD(seq)
  }

  implicit class RichSQLContext(private val sqlContext: SQLContext) {
    def apply(sqlText: String): DataFrame = sqlContext.sql(sqlText)
  }

  implicit class RichPairRDD[K, V](private val rdd: RDD[(K, V)]) {
    def filterByKey(f: K => Boolean): RDD[(K, V)] = rdd.filter(t => f(t._1))

    def filterByValue(f: V => Boolean): RDD[(K, V)] = rdd.filter(t => f(t._2))

    def mapKeys[U](f: K => U): RDD[(U, V)] = rdd.map(t => (f(t._1), t._2))

    def flatMapKeys[U](f: K => TraversableOnce[U]): RDD[(U, V)] = rdd.flatMap(t => f(t._1).map((_, t._2)))
  }

  implicit class RichDate(private val d: Date) {
    def between(start: Date, end: Date): Boolean  = {
      require(start.before(end))
      !(d.before(start) | end.after(end))
    }

  }

}
