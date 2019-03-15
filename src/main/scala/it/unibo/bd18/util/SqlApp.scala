package it.unibo.bd18.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SqlApp extends SparkAppBase {

  protected[this] final lazy val spark = {
    require(conf != null, "conf cannot be null")
    SparkSession.builder.config(conf).getOrCreate()
  }

  protected[this] override final def sc: SparkContext = spark.sparkContext

  protected[this] final def sqlContext: SQLContext = spark.sqlContext

}
