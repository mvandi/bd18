package it.unibo.bd18.lab4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

object Exercise4 extends Lab4Base {

  override protected[this] val batchDuration = Seconds(3)

  ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    .window(Seconds(30), Seconds(3))
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)
    .map(_.swap)
    .transform(_.sortByKey(false))
    .print()

  ssc.start()

  ssc.stop()


}
