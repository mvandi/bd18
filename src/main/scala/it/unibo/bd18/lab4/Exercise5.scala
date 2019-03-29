package it.unibo.bd18.lab4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Minutes, Seconds}

object Exercise5 extends Lab4Base {

  override protected[this] val batchDuration = Seconds(3)

  ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    .window(Minutes(1), Seconds(10))
    .filter(_.nonEmpty)
    .map( _.split("\\s\\|\\s"))
    .flatMap(_(2).split(",\\s"))
    .filter(_.nonEmpty)
    .map((_, 1))
    .reduceByKey(_ + _)
    .map(_.swap)
    .transform(_.sortByKey(ascending = false))
    .print()

  ssc.start()

  ssc.stop()


}
