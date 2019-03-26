package it.unibo.bd18.stackoverflow

import it.unibo.bd18.app.SQLApp
import it.unibo.bd18.stackoverflow.QuestionData.df
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object PreProcessing extends SQLApp {

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("PreProcessing App")

  import it.unibo.bd18.util.implicits._
  import implicits._

  val questionsSrcPath: String = ???
  val questionTagsSrcPath: String = ???

  val questionsDestPath: String = ???
  val questionTagsDestPath: String = ???

  val questionsRDD = loadCSV(questionsSrcPath).map(QuestionData.extract)
  val questionTagsRDD = loadCSV(questionTagsSrcPath).map(QuestionTagData.extract)

  val startDate = sc.broadcast(df.parse("2012-01-01T00:00:00Z"))
  val endDate = sc.broadcast(df.parse("2017-01-01T00:00:00Z"))

  val (questions, questionTags) = questionsRDD
    .filter(x => x.creationDate.between(startDate.value, endDate.value))
    .keyBy(_.id)
    .partitionBy(new HashPartitioner(8))
    .join(questionTagsRDD.keyBy(_.id))
    .map(_._2)
    .collect
    .unzip

  loadCSVHeader(questionsSrcPath).union(questions.toSeq.toRDD.map(_.toCSVString)).saveAsTextFile(questionsDestPath)
  loadCSVHeader(questionTagsSrcPath).union(questionTags.toSeq.toRDD.map(_.toCSVString)).saveAsTextFile(questionTagsDestPath)

  private def loadCSV(path: String, header: Boolean = true): RDD[Row] = spark.read.format("csv").option("header", header.toString).csv(path).rdd

  private def loadCSVHeader(path: String): RDD[String] = loadCSV(path, header = false).take(1).toSeq.toRDD.map(_.mkString(","))

}
