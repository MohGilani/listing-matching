package com.mhsg.hopper.transformations

import com.mhsg.hopper.BaseSuite

class UDFsSuite extends BaseSuite {
  "GeoHashHelper.cleanLatitudeUnsafe" should "return result on valid latitude" in {
    val validLat1 = "89.9999"
    val validLat2 = "-89.9999"
    UDFs.GeoHashHelper.cleanLatitudeUnsafe(validLat1) shouldBe validLat1
    UDFs.GeoHashHelper.cleanLatitudeUnsafe(validLat2) shouldBe validLat2
  }

  it should "return null for invalid latitude" in {
    val validLat1 = "90.00001"
    val validLat2 = "-90.00001"
    UDFs.GeoHashHelper.cleanLatitudeUnsafe(validLat1) shouldBe null
    UDFs.GeoHashHelper.cleanLatitudeUnsafe(validLat2) shouldBe null
  }

  "GeoHashHelper.cleanLongitudeUnsafe" should "return result on valid longitude" in {
    val validLng1 = "179.9999"
    val validLng2 = "-179.9999"
    UDFs.GeoHashHelper.cleanLongitudeUnsafe(validLng1) shouldBe validLng1
    UDFs.GeoHashHelper.cleanLongitudeUnsafe(validLng2) shouldBe validLng2
  }

  it should "return null for invalid longitude" in {
    val validLng1 = "180.00001"
    val validLng2 = "-180.00001"
    UDFs.GeoHashHelper.cleanLongitudeUnsafe(validLng1) shouldBe null
    UDFs.GeoHashHelper.cleanLongitudeUnsafe(validLng2) shouldBe null
  }

  "GeoHashHelper.geoHash" should "generate valid geo hash with different precisions" in {
    val lat = "29.001"
    val lng = "170.1039"
    UDFs.GeoHashHelper.geoHash(lat, lng, 1) shouldBe "x"
    UDFs.GeoHashHelper.geoHash(lat, lng, 2) shouldBe "xv"
    UDFs.GeoHashHelper.geoHash(lat, lng, 3) shouldBe "xv0"
    UDFs.GeoHashHelper.geoHash(lat, lng, 4) shouldBe "xv0u"
    UDFs.GeoHashHelper.geoHash(lat, lng, 5) shouldBe "xv0uy"
    UDFs.GeoHashHelper.geoHash(lat, lng, 12) shouldBe "xv0uyz3zh4jp"
  }

  it should "generate valid geo hash for valid (lat,lng) pairs" in {
    val lat1 = "39.801"
    val lng1 = "10.973911"
    val lat2 = "-1.001"
    val lng2 = "139.000001"
    val lat3 = "79.002801"
    val lng3 = "-173.009"
    val lat4 = "-36.49"
    val lng4 = "-11.1111"

    UDFs.GeoHashHelper.geoHash(lat1, lng1, 12) shouldBe "sppf3twcwsjh"
    UDFs.GeoHashHelper.geoHash(lat2, lng2, 12) shouldBe "rpff71b8yb0f"
    UDFs.GeoHashHelper.geoHash(lat3, lng3, 12) shouldBe "bnhcrn48wje2"
    UDFs.GeoHashHelper.geoHash(lat4, lng4, 12) shouldBe "7c807jjnkdgr"
  }

  "StringHelper.toUpperCase" should "make a non-empty string uppercase" in {
    val str = "Hi HOW, are YoU doINg_myFriend?"
    UDFs.StringHelper.toUpperCase(str) shouldBe "HI HOW, ARE YOU DOING_MYFRIEND?"
  }

  it should "return null for null str" in {
    val str = null
    UDFs.StringHelper.toUpperCase(str) shouldBe null
  }

  "StringHelper.trim" should "trim a non-empty string" in {
    val str = "  \t my friend \n"
    UDFs.StringHelper.trim(str) shouldBe "my friend"
  }

  it should "return null for a null string" in {
    val str = null
    UDFs.StringHelper.trim(str) shouldBe null
  }
}
