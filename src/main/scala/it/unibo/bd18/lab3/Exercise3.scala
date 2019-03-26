package it.unibo.bd18.lab3

object Exercise3 extends Lab3Base {

  rddWeather
    .filter(_.temperature < 999)
    .keyBy(x => x.usaf + x.wban)
    .join(rddStation
      .keyBy(x => x.usaf + x.wban))

}
