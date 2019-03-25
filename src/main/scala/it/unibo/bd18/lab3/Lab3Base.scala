package it.unibo.bd18.lab3

import it.unibo.bd18.app.SparkApp

private[lab3] trait Lab3Base extends SparkApp {

  protected[this] lazy val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract).cache
  protected[this] lazy val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract).cache

}
