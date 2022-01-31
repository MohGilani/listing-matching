package com.mhsg.hopper

class SchemaSuite extends BaseSuite {

  "mirrorCol" should "add hyphen to a the beginning" in {
    val colName = "name"
    Schema.mirrorCol(colName) shouldBe "_name"
  }

  "buildMirrorSchema" should "add hyphen to a list of names" in {
    val colNames = List("name", "last_name", "01/02/2022")
    Schema.buildMirrorSchema(colNames) shouldBe List("_name", "_last_name", "_01/02/2022")
  }

  "dateColumns" should "filter columns with / in their name" in {
    val colNames = List("name", "last_name", "01/02/2022")
    Schema.dateColumns(colNames) shouldBe List("01/02/2022")
  }

}
