/*
////// Setup

val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

////// Exercise 1

// We want the average AND the maximum temperature registered for every month
rddWeather.filter(_.temperature<999).map(x => (x.month, x.temperature)).aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1),(a1,a2)=>(a1._1+a2._1,a1._2+a2._2)).map({case(k,v)=>(k,v._1/v._2)}).collect()
rddWeather.filter(_.temperature<999).map(x => (x.month, x.temperature)).reduceByKey((x,y)=>{if(x<y) y else x}).collect()
// Optimize the two jobs by avoiding the repetition of the same computations and by defining a good number of partitions
// Hints:
// - Verify your persisted data in the web UI
// - Use either repartition() or coalesce() to define the number of partitions
// - repartition() shuffles all the data
// - coalesce() minimizes data shuffling by exploiting the existing partitioning
// - Verify the execution plan of your RDDs with rdd.toDebugString

////// Exercise 2

import org.apache.spark.HashPartitioner
val p = new HashPartitioner(8)

// Consider the following commands to transform the Station RDD:
val rddS1 = rddStation.partitionBy(p).keyBy(x => x.usaf + x.wban).cache()
val rddS2 = rddStation.partitionBy(p).cache().keyBy(x => x.usaf + x.wban) 
val rddS3 = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(p).cache()
val rddS4 = rddStation.keyBy(x => x.usaf + x.wban).cache().partitionBy(p)
// Which of these options is better? And why?

////// Exercise 3

// Join the Weather and Station RDDs
rddWeather.filter(_.temperature<999)
rdd1.join(rdd2)
// Hints & considerations:
// - Join syntax: rdd1.join(rdd2)
// - Both RDDs should be structured as key-value RDDs with the same key: usaf + wban
// - Consider partitioning and caching to optimize the join
// - Careful: it is not enough for the two RDDs to have the same number of partitions; they must have the same partitioner!
// - Verify the execution plan of the join in the web UI


////// Exercise 4

// Given Exercise 3, compute the maximum temperature for every city 
// Given Exercise 3, compute the maximum temperature for every city in Italy
StationData.country == "IT"
// Sort the results by descending temperature
map({case(k,v)=>(v,k)}) 

////// Exercise 5

// Clean the cache
sc.getPersistentRDDs.foreach(_._2.unpersist())

// Use Spark's web UI to verify the space occupied by the following RDDs:
import org.apache.spark.storage.StorageLevel._
val memRdd = rddWeather.sample(false,0.1).repartition(8).cache()
val memSerRdd = memRdd.map(x=>x).persist(MEMORY_ONLY_SER)
val diskRdd = memRdd.map(x=>x).persist(DISK_ONLY)

memRdd.collect()
memSerRdd.collect()
diskRdd.collect()

////// Exercise 6

val rddW = rddWeather.sample(false,0.1).filter(_.temperature<999).keyBy(x => x.usaf + x.wban).cache()
val rddS = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(new HashPartitioner(8)).cache()
rddW.collect
rddS.collect

// Is it better to simply join the two RDDs..
rddW.join(rddS).filter(_._2._2.country=="IT").map({case(k,v)=>(v._2.name,v._1.temperature)}).reduceByKey((x,y)=>{if(x<y) y else x}).collect()

// ..to enforce on rddW1 the same partitioner of rddS..
rddW.partitionBy(new HashPartitioner(8)).join(rddS).filter(_._2._2.country=="IT").map({case(k,v)=>(v._2.name,v._1.temperature)}).reduceByKey((x,y)=>{if(x<y) y else x}).collect()

// ..or to exploit broadcast variables?
val bRddS = sc.broadcast(rddS.collectAsMap())
val rddJ = rddW.map({case (k,v) => (bRddS.value.get(k),v)}).filter(_._1!=None).map({case(k,v)=>(k.get.asInstanceOf[StationData],v)})
rddJ.filter(_._1.country=="IT").map({case (k,v) => (k.name,v.temperature)}).reduceByKey((x,y)=>{if(x<y) y else x}).collect()
*/
