package util

import org.apache.spark.SparkContext

trait SparkApp extends SparkAppBase {

  protected[this] lazy val sc = {
    require(conf != null, "conf cannot be null")
    new SparkContext(conf)
  }

}
