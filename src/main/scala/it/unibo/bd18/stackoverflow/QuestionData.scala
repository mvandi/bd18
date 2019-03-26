package it.unibo.bd18.stackoverflow

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row

import QuestionData.df

trait QuestionData {

  def id(): Int

  def creationDate(): Date

  def closedDate(): Option[Date]

  def deletionDate(): Option[Date]

  def score(): Int

  def ownerUserId(): Option[Int]

  def answerCount(): Int

  def toCSVString(): String = s"$id,${df.format(creationDate)},${closedDate.map(df.format).getOrElse("NA")},${deletionDate.map(df.format).getOrElse("NA")},$score,${ownerUserId.map(_.toString).getOrElse("NA")},$answerCount"

}

object QuestionData {

  def extract(row: Row): QuestionData = QuestionDataImpl(
    getId(row),
    getCreationDate(row),
    getClosedDate(row),
    getDeletionDate(row),
    getScore(row),
    getOwnerUserId(row),
    getAnswerCount(row))

  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  private def getId(row: Row): Int = row.getInt(0)

  private def getCreationDate(row: Row): Date = df.parse(row.getString(1))

  private def getClosedDate(row: Row): Option[Date] = getOption(row)(_.getString(2))(_ != "NA").map(df.parse)

  private def getDeletionDate(row: Row): Option[Date] = getOption(row)(_.getString(3))(_ != "NA").map(df.parse)

  private def getScore(row: Row): Int = row.getInt(4)

  private def getOwnerUserId(row: Row): Option[Int] = getOption(row)(_.getString(5))(_ != "NA").map(_.toInt)

  private def getAnswerCount(row: Row): Int = row.getInt(6)

  private def getOption[T](row: Row)(f: Row => T)(g: T => Boolean): Option[T] = Some(f(row)).filter(g)

  private case class QuestionDataImpl(
                                       override val id: Int,
                                       override val creationDate: Date,
                                       override val closedDate: Option[Date],
                                       override val deletionDate: Option[Date],
                                       override val score: Int,
                                       override val ownerUserId: Option[Int],
                                       override val answerCount: Int
                                     ) extends QuestionData

}
