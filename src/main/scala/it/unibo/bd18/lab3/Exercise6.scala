package it.unibo.bd18.lab3

import org.apache.spark.HashPartitioner

object Exercise6 extends Lab3Base {

  import it.unibo.bd18.util.implicits._

  val rddW = rddWeather
    .sample(withReplacement = false, fraction = 0.1)
    .filter(_.temperature < 999)
    .keyBy(x => x.usaf + x.wban)
    .cache()

  val rddS = rddStation
    .keyBy(x => x.usaf + x.wban)
    .partitionBy(new HashPartitioner(8))
    .cache()

  // simply join the two RDDs
  val rddSol1 = rddW.join(rddS)

  // enforce on rddW the same partitioner of rddS (and then join)
  val rddSol2 = rddW
    .partitionBy(new HashPartitioner(8))
    .join(rddS)

  // exploit broadcast variables
  val broadcastS = sc.broadcast(rddS.collectAsMap())
  val rddSol3 = rddW
    .mapPair((k, v) => (v, broadcastS.value.get(k)))
    .flatten
    .map(x => (x._1.usaf + x._1.wban, x))

  rddSol3
    .filter(_._2._2.country == "IT")
    .mapPair((k, v) => (v._2.name, v._1.temperature))
    .reduceByKey { (x, y) => if (x < y) y else x }
    .collect()

}
