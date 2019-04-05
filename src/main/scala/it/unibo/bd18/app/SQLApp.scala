package it.unibo.bd18.app

import org.apache.spark.sql.{DataFrame, SQLContext}

trait SQLApp extends SparkApp {

  protected[this] final def sqlContext: SQLContext = spark.sqlContext

  protected[this] final def sql(sqlText: String): DataFrame = spark.sql(sqlText)

}
