package com.test.spark

import org.apache.spark._
import org.apache.log4j._

object FriendsByFirstLetter {
  /** A function that splits a line of input into (firstLetter, numFriends) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val firstLetter = fields(1).charAt(0)
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (firstLetter, numFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByFirstLetter")

    val lines = sc.textFile("fakefriends.csv")
    val rdd = lines.map(parseLine)

    val totalByFirstLetter = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val averageByFirstLetter = totalByFirstLetter.mapValues((x => x._1 / x._2))

    averageByFirstLetter.collect().foreach(println)


  }
}
