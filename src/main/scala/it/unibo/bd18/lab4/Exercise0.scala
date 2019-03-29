package it.unibo.bd18.lab4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds

object Exercise0 extends Lab4Base {

  override protected[this] val batchDuration = Seconds(3)

  ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    .flatMap(_.split(""))
    .count()
    .print()
  ssc.start()

  ssc.stop()

}
