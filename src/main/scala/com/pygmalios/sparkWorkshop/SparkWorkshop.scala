package com.pygmalios.sparkWorkshop

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Mix-in trait with useful utilities for Spark apps.
  */
trait SparkWorkshop {
  def withSpark(action: (SparkContext) => Unit): Unit = {
    // Create Spark context
    val sc = new SparkContext(SparkWorkshop.sparkConf)

    // Invoke action with created Spark context
    action(sc)

    // Stop Spark context once we're done
    sc.stop()
  }
}

object SparkWorkshop {
  // Create Spark context
  val sparkConf = new SparkConf()
    .setAppName("Pygmalios Spark workshop")
    .setMaster("local[*]")
}
