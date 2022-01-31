package com.mhsg.hopper

object Schema {
  val ZipcodeCol = "zipcode"
  val PlatformCol = "platform"
  val LatCol = "lat"
  val LngCol = "lng"
  val BedroomsCol = "bedrooms"
  val BathroomsCol = "bathrooms"
  val BedsCol = "beds"
  val CapacityCol = "capacity"
  val GeoHashCol = "geoHash"
  val ListingIdCol = "listing_ID"
  val AverageDailyRateCol = "adr"
  val AverageOccupancyCol = "avg_occ"
  val PrimaryDomainCol = "pm_domain_name"

  val IdCol = "id"
  val ScoreCol = "score"
  val GroupCol = "groupCol"
  val GroupIdCol = "GroupIdCol"

  def mirrorCol(colName: String): String = s"_$colName"

  def buildMirrorSchema(colNames: Seq[String]): Seq[String] = colNames.map(mirrorCol)

  def dateColumns(colNames: Seq[String]): Seq[String] = colNames.filter(_.contains("/"))
}
