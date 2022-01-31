package com.mhsg.hopper.steps

import com.mhsg.hopper.Schema._
import com.mhsg.hopper.transformations.UDFs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Read {
  private val dataFileReaderOptions = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "nullValue" -> "NA",
    "mode" -> "DROPMALFORMED",
    "treatEmptyValuesAsNulls" -> "true"
  )

  val crawledFileReaderOptions1 = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "nullValue" -> "null",
    "mode" -> "DROPMALFORMED",
    "treatEmptyValuesAsNulls" -> "true"
  )

  def readInData(spark: SparkSession, dataPath: String, homeAwayCrawledPath: String,
                 bookingCrawledPath: String): DataFrame = {
    val listingsDF = spark.read.format("csv")
      .options(dataFileReaderOptions)
      .load(dataPath)

    val homeAwayCrawledDF = spark.read.format("csv")
      .options(crawledFileReaderOptions1)
      .load(homeAwayCrawledPath)

    val bookingCrawledDF = spark.read.format("csv")
      .options(crawledFileReaderOptions1)
      .load(bookingCrawledPath)

    val cleanedHomeAwayCrawledDF = cleanHomeAwayCrawledData(homeAwayCrawledDF)
    val cleanedBookingCrawledDF = cleanBookingCrawledData(bookingCrawledDF)
    val cleanedListingDF = cleanListingData(listingsDF)
    val enrichedListingDF = enrichListingData(cleanedListingDF, cleanedHomeAwayCrawledDF, cleanedBookingCrawledDF)

    enrichedListingDF
  }

  private def cleanBookingCrawledData(df: DataFrame): DataFrame =
    df.withColumn(LatCol, UDFs.cleanLatitudeUDF(col(LatCol)))
      .withColumn(LngCol, UDFs.cleanLongitudeUDF(col(LngCol)))

  private def cleanHomeAwayCrawledData(df: DataFrame): DataFrame =
    df.withColumn(LatCol, UDFs.cleanLatitudeUDF(col(LatCol)))
      .withColumn(LngCol, UDFs.cleanLongitudeUDF(col(LngCol)))

  private def cleanListingData(df: DataFrame): DataFrame =
    dateColumns(df.columns)
      .foldLeft(df)((tmpDF, colName) => tmpDF.withColumn(colName, UDFs.strTrimUDF(col(colName))))
      .withColumn(AverageDailyRateCol, UDFs.strTrimUDF(col(AverageDailyRateCol)))
      .withColumn(AverageOccupancyCol, UDFs.strTrimUDF(col(AverageOccupancyCol)))
      .withColumn(PrimaryDomainCol, UDFs.strTrimUDF(col(PrimaryDomainCol)))
      .withColumn(CapacityCol, UDFs.strTrimUDF(col(CapacityCol)))
      .withColumn(BedroomsCol, UDFs.strTrimUDF(col(BedroomsCol)))
      .withColumn(BathroomsCol, UDFs.strTrimUDF(col(BathroomsCol)))
      .withColumn(BedsCol, UDFs.strTrimUDF(col(BedsCol)))
      .withColumn(ZipcodeCol, UDFs.strToUpperCaseUDF(col(ZipcodeCol)))
      .withColumn(ZipcodeCol, UDFs.strTrimUDF(col(ZipcodeCol)))
      .withColumn(PlatformCol, UDFs.strToUpperCaseUDF(col(PlatformCol)))
      .withColumn(PlatformCol, UDFs.strTrimUDF(col(PlatformCol)))
      .withColumn(LatCol, UDFs.cleanLatitudeUDF(col(LatCol)))
      .withColumn(LngCol, UDFs.cleanLongitudeUDF(col(LngCol)))

  private def enrichListingData(listingDF: DataFrame, homeAwayDF: DataFrame, bookingDF: DataFrame): DataFrame = {
    def enrichWithCrawledData(mainDF: DataFrame, crawledDF: DataFrame): DataFrame = {
      val crawledNewColName = crawledDF.columns.map(c => if (c != ListingIdCol) s"_$c" else c)
      val renamedCrawledDF = crawledDF.toDF(crawledNewColName: _*)
      val joinedDF = mainDF.join(renamedCrawledDF, Seq(ListingIdCol), "left_outer")

      val finalCols = mainDF.columns.map { c =>
        if (crawledDF.columns.contains(c) && c != ListingIdCol)
          when(col(c).isNotNull, col(c)).otherwise(col(s"_$c")).as(c)
        else
          col(c).as(c)
      }
      joinedDF.select(finalCols: _*)
    }

    val enrichedWithCrawledData1 = enrichWithCrawledData(listingDF, homeAwayDF)
    val enrichedWithCrawledData2 = enrichWithCrawledData(enrichedWithCrawledData1, bookingDF)
    enrichedWithCrawledData2.withColumn(GeoHashCol, UDFs.calculateGeoHashUDF(col(LatCol), col(LngCol), lit(9)))
  }
}
