package com.pygmalios.sparkWorkshop.excercise01

import com.pygmalios.sparkWorkshop.ExcerciseResources
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Excercise 1: word count
  */
object Excercise01App extends App {
  // Create Spark context
  val sparkConf = new SparkConf()
    .setAppName("Excercise 01: Word count")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  //=====================================================================================
  // Create RDD from a data source
  //=====================================================================================

  // Read data from a provided text file to RDD consisting of 10 partitions
  val textFile: RDD[String] = sc.textFile(ExcerciseResources.data01.toString, 10)

  //=====================================================================================
  // Define transformations on the RDD
  //=====================================================================================

  // Count number of occurence for all words in the text
  val counts: RDD[(String, Int)] = textFile
    // Split lines into individual words
    .flatMap(line => line.split(" "))
    // Create key-value tuples from all words, where "word" is the key and value is 1
    .map(word => (word, 1))
    // Reduce tuples with the same key (word) using addition
    .reduceByKey(_ + _)

  // TODO task 1.1: print counts to console (hint: RDD.foreach, println)
  // TODO task 1.2: read another text file data02 and join the results
  // TODO task 1.3: print count sorted by number of occurenece (hint: RDD.sortBy, RDD.repartition)
  // TODO task 1.4: save results formatted as Json { "word1": count1, "word2": count2, ... } to a text file (hint: RDD.saveAsTextFile)

  // Stop Spark context
  sc.stop()
}