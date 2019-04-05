package it.unibo.bd18.lab5

import it.unibo.bd18.app.SQLApp

object Exercise1 extends SQLApp {

  import org.apache.spark.sql.types.{StructField, StructType}
  import org.apache.spark.sql.{Row, types}

  val moviesDF = spark.read.json("/bigdata/dataset/movies")

  moviesDF limit 10 show()

  val populationDF = {
    val rowRDD = sc.textFile("/bigdata/dataset/population")
      .map(_.split("\\s*;\\s*").toSeq.filter(_.nonEmpty))
      .map(Row.fromSeq)

    val schema = StructType(
      Seq(
        StructField(name = "zipCode", dataType = types.IntegerType),
        StructField(name = "avgAge", dataType = types.FloatType),
        StructField(name = "totalPopulation", dataType = types.IntegerType),
        StructField(name = "totalMen", dataType = types.IntegerType),
        StructField(name = "totalWomen", dataType = types.IntegerType)
      )
    )
    spark.createDataFrame(rowRDD, schema)
  }

  populationDF limit 10 show()

  val realEstateDF = spark.read
    .option("delimiter", ";")
    .option("header", "true")
    .csv("/bigdata/dataset/real_estate")

  realEstateDF limit 10 show()

  val userDataDF = sql("SELECT * FROM parquet.`/bigdata/dataset/userdata`")

  userDataDF limit 10 show()

}
