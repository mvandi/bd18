package it.unibo.bd18.lab3

object Exercise1 extends Lab3Base {

  import it.unibo.bd18.util.implicits._

  val NUM_PARTITIONS = 3 * sc.coreCount

  println(s"Number of partitions = $NUM_PARTITIONS")

  val rddPartitioned = rddWeather
    .coalesce(NUM_PARTITIONS)
    .filter(_.temperature < 999)
    .map(x => (x.month, x.temperature))
    .cache()

  def printResults(results: Map[_, _]): Unit = results.foreach(x => println(s"${x._1} -> ${x._2}"))

  println("Average temperature per month")
  val rddAvg = rddPartitioned
    .aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))
    .mapValues(v => v._1 / v._2)
  println(rddAvg.toDebugString)
  printResults(rddAvg.collect().toMap)

  println("Maximum temperature per month")
  val rddMax = rddPartitioned.reduceByKey(_.max(_))
  println(rddMax.toDebugString)
  printResults(rddMax.collect().toMap)

}
