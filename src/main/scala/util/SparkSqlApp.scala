package util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkSqlApp extends SparkAppBase {

  protected[this] lazy val spark = {
    require(conf != null, "conf cannot be null")
    SparkSession.builder.config(conf).getOrCreate()
  }

  protected[this] def sc: SparkContext = spark.sparkContext

  protected[this] def sqlContext: SQLContext = spark.sqlContext

}
