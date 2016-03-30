package com.pygmalios.sparkWorkshop

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Excercise 2: streaming word count
  *
  * To be able to run this you have to start NetCat utility on port 9999:
  * $ nc -lk 9999
  */
object Excercise02App extends App {
  // Create Spark streaming context
  val ssc = new StreamingContext(SparkWorkshop.sparkConf, Seconds(1))

  // Read lines from TCP socket
  val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  // TODO task 2.1: we must have an output action (hint: print)

  // TODO task 2.2: use Queue instead of socket stream and read date01.txt file , hint:
  //  val queue = new mutable.Queue[RDD[String]]()
  //  queue.enqueue(ssc.sparkContext.parallelize(List("a", "b")))
  //  val lines = ssc.queueStream(queue)

  // TODO task 2.3: implement word count

  // TODO task 2.4: check localhost:4040 in web browser while running

  // TODO task 2.5: implement all tasks from excercise 1

  // Start and wait until killed
  ssc.start()

  ssc.awaitTermination()
}
