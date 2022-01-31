package com.mhsg.hopper

import com.mhsg.hopper.steps._
import org.apache.spark.sql.SparkSession

object ListingMatching {

  val sampleDataFilePath = "./data/sample.csv"
  val homeAwayCrawledDataFilePath = "./data/home_away_crawled_result.csv"
  val bookingCrawledDataFilePath = "./data/booking_crawled_result.csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("com_mhsg_hopper_ListingMatching")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val rawData = Read.readInData(spark, sampleDataFilePath, homeAwayCrawledDataFilePath, bookingCrawledDataFilePath)
    val processedData = Process.processData(spark, rawData)
    processedData.show(1000, false)

    spark.close()
  }
}
