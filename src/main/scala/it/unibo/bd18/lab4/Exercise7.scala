package it.unibo.bd18.lab4

import org.apache.spark.streaming.Seconds

object Exercise7 extends Lab4Base {

  override protected[this] val batchDuration = Seconds(3)
/*
  def updateFunction( newValues: Seq[(Int,Int)], oldValue: Option[(Int,Int,Int,Double)] ): Option[(Int,Int,Int,Double)] = {
    oldValue.getOrElse(0,0,0,0.0) // to initialize oldValue
    newValues.map(_._1).sum // to sum the first elements of newValues
    // ...
    Some((totTweets, totSentiment, countSentiment, avgSentiment))
  }
*/
  ssc.start()

  ssc.stop()

}
