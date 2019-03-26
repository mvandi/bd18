package it.unibo.bd18.lab3

object Exercise2 extends Lab3Base {

  import org.apache.spark.HashPartitioner
  val p = new HashPartitioner(8)

  // Consider the following commands to transform the Station RDD:
//  val rddS1 = rddStation.partitionBy(p).keyBy(x => x.usaf + x.wban).cache()
//  val rddS2 = rddStation.partitionBy(p).cache().keyBy(x => x.usaf + x.wban)
  val rddS3 = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(p).cache()
//  val rddS4 = rddStation.keyBy(x => x.usaf + x.wban).cache().partitionBy(p)

}
