package it.unibo.bd18.lab4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds}

object Exercise1 extends Lab4Base {

  override protected[this] val batchDuration: Duration = Seconds(3)

  ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)
    .map(_.swap)
    .transform(_.sortByKey(ascending = false))
    .print()

  ssc.start()

  ssc.stop()

}
