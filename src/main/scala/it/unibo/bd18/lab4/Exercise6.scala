package it.unibo.bd18.lab4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Minutes, Seconds}

object Exercise6 extends Lab4Base {

  override protected[this] val batchDuration = Seconds(1)

  def updateFunction(newValues: Seq[Int], oldValue: Option[Int]): Option[Int] = {
    Some(oldValue.getOrElse(0) + newValues.sum)
  }

  ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    .window(Minutes(1), Seconds(10))
    .filter(_.nonEmpty)
    .map( _.split("\\s\\|\\s"))
    .map(_(4))
    .filter(x => x.nonEmpty && x != "0")
    .map((_, 1))
    .updateStateByKey(updateFunction)
    .reduceByKey(_ + _)
    .map(_.swap)
    .transform(_.sortByKey(ascending = false))
    .print()

  ssc.start()

  ssc.stop()

}
