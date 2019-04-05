package it.unibo.bd18.lab5

import it.unibo.bd18.app.SQLApp

object Exercise2 extends SQLApp {

  import org.apache.spark.sql.types.{StructField, StructType}
  import org.apache.spark.sql.{Row, types}

  val populationDF = {
    val rowRDD = sc.textFile("/bigdata/dataset/population")
      .map(_.split("\\s*;\\s*").toSeq.filter(_.nonEmpty))
      .map(Row.fromSeq)

    val schema = StructType(
      Seq(
        StructField(name = "zipCode", dataType = types.StringType),
        StructField(name = "avgAge", dataType = types.StringType),
        StructField(name = "totalPopulation", dataType = types.StringType),
        StructField(name = "totalMen", dataType = types.StringType),
        StructField(name = "totalWomen", dataType = types.StringType)
      ))
    spark.createDataFrame(rowRDD, schema)
  }

  populationDF.write json "/user/mvandi/bigdata/lab5/ex2/population"

  val userDataDF = sql("SELECT * FROM parquet.`/bigdata/dataset/userdata`")

  userDataDF.write saveAsTable "userdata_mvandi"

  val moviesDF = spark.read.json("/bigdata/dataset/movies")

  moviesDF.write parquet "/user/mvandi/bigdata/lab5/ex2/movies"

}
