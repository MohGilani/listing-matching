package com.mhsg.hopper.transformations

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.{Failure, Success, Try}

object UDFs {
  object GeoHashHelper {

    import ch.hsr.geohash.GeoHash

    private val maxLatitude = 90.0
    private val minLatitude = -90.0
    private val maxLongitude = 180.0
    private val minLongitude = -180.0

    private def clean(inValue: String, minValue: Double, maxValue: Double): Option[String] =
      Try(inValue.toDouble) match {
        case Failure(_) => None
        case Success(v) if v > maxValue || v < minValue => None
        case Success(v) => Some(v.toString)
      }

    val cleanLatitudeUnsafe: String => String =
      (latitude: String) => clean(latitude, minLatitude, maxLatitude).orNull

    val cleanLongitudeUnsafe: String => String =
      (longitude: String) => clean(longitude, minLongitude, maxLongitude).orNull

    val geoHash: (String, String, Int) => String =
      (lat: String, lon: String, precision: Int) =>
        Try(GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, lon.toDouble, precision)).getOrElse(null)
  }

  object StringHelper {
    val toUpperCase: String => String =
      (inStr: String) => Try(inStr.toUpperCase).getOrElse(null)

    val trim: String => String =
      (inStr: String) => Try(inStr.trim).getOrElse(null)

  }

  val calculateGeoHashUDF: UserDefinedFunction = udf(GeoHashHelper.geoHash)
  val cleanLatitudeUDF: UserDefinedFunction = udf(GeoHashHelper.cleanLatitudeUnsafe)
  val cleanLongitudeUDF: UserDefinedFunction = udf(GeoHashHelper.cleanLongitudeUnsafe)

  val strToUpperCaseUDF: UserDefinedFunction = udf(StringHelper.toUpperCase)
  val strTrimUDF: UserDefinedFunction = udf(StringHelper.trim)
}
