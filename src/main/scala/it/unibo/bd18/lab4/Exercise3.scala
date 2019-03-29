package it.unibo.bd18.lab4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds}

object Exercise3 extends Lab4Base {

  override protected[this] val batchDuration: Duration = Seconds(3)

  def updateFunction(newValues: Seq[Int], oldValue: Option[Int]): Option[Int] = {
    Some(oldValue.getOrElse(0) + newValues.sum)
  }

  ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    .flatMap(_.split(" "))
    .map((_, 1))
    .updateStateByKey(updateFunction)
    .map(_.swap)
    .transform(_.sortByKey(ascending = false))
    .print()
  ssc.checkpoint("hdfs:/user/mvandi/streaming/checkpoint3")

  ssc.start()

  ssc.stop()

}
