package it.unibo.bd18.lab3

object Exercise5 extends Lab3Base {

  sc.getPersistentRDDs.foreach(_._2.unpersist())

  import org.apache.spark.storage.StorageLevel

  val memRdd = rddWeather.sample(withReplacement = false, 0.1).repartition(8).cache()
  val memSerRdd = memRdd.map(identity).persist(StorageLevel.MEMORY_ONLY_SER)
  val diskRdd = memRdd.map(identity).persist(StorageLevel.DISK_ONLY)

  memRdd.collect()
  memSerRdd.collect()
  diskRdd.collect()

}
