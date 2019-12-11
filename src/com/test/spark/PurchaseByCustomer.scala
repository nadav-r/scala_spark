package com.test.spark

import org.apache.spark._
import org.apache.log4j._
object PurchaseByCustomer {

  def parseLine(line: String): (Int, Double) = {
    val fields = line.split(",");
    val customerID: Int = fields(0).toInt;
    val amountSpent: Double = fields(2).toDouble;
    (customerID, amountSpent)
  }



  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "FriendsByAge");
    val lines = sc.textFile("customer-orders.csv");
    val rdd = lines.map(parseLine);
    val totalByCustomer = rdd.reduceByKey((x,y)=> x+y)
    val totalByCustomerSorted = totalByCustomer.map(x=> (x._2,x._1)).sortByKey(true)


    val totalByCustomerRounded = totalByCustomerSorted.map(x => (x._2, "%.2f".format(x._1)))

    val results = totalByCustomerRounded.collect()
    results.foreach(println)
  }
}