package it.unibo.bd18.lab3

import org.apache.spark.rdd.RDD

object Exercise4 extends Lab3Base {

  import it.unibo.bd18.util.implicits._

  val res: RDD[(String, (WeatherData, StationData))] = rddWeather
    .filter(_.temperature < 999)
    .keyBy(x => x.usaf + x.wban)
    .join(rddStation.keyBy(x => x.usaf + x.wban))
    .filterByValue(_._2.country == "IT")
    .sortBy(_._2._1.temperature, ascending = false)

}
