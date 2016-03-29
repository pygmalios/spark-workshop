package com.pygmalios.sparkWorkshop.excercise01

import com.pygmalios.sparkWorkshop.ExcerciseResources
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Word count.
  */
object Excercise01App extends App {
  // Create Spark context
  val sparkConf = new SparkConf()
    .setAppName("Excercise 01: Word count")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val textFile = sc.textFile(ExcerciseResources.data01.toString)
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)

  sc.stop()
}
