package com.mhsg.hopper.steps

import com.mhsg.hopper.Schema._
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Process {
  private val baseJoinCondition: Column = col(IdCol) =!= col(mirrorCol(IdCol)) &&
    col(PlatformCol) =!= col(mirrorCol(PlatformCol)) &&
    col(ZipcodeCol) === col(mirrorCol(ZipcodeCol))

  private val bedroomsMatches = col(BedroomsCol).isNotNull &&
    col(mirrorCol(BedroomsCol)).isNotNull &&
    col(BedroomsCol) === col(mirrorCol(BedroomsCol))

  private val bathroomsMatches = col(BathroomsCol).isNotNull &&
    col(mirrorCol(BathroomsCol)).isNotNull &&
    col(BathroomsCol) === col(mirrorCol(BathroomsCol))

  private val geoHashMatches = col(GeoHashCol).isNotNull &&
    col(mirrorCol(GeoHashCol)).isNotNull &&
    col(GeoHashCol) === col(mirrorCol(GeoHashCol))

  private val averageDailyRateMatches = col(AverageDailyRateCol).isNotNull &&
    col(mirrorCol(AverageDailyRateCol)).isNotNull &&
    col(AverageDailyRateCol).cast("double") > col(mirrorCol(AverageDailyRateCol)).cast("double") - 50 &&
    col(AverageDailyRateCol).cast("double") < col(mirrorCol(AverageDailyRateCol)).cast("double") + 50

  private val averageOccupancyMatches = col(AverageOccupancyCol).isNotNull &&
    col(mirrorCol(AverageOccupancyCol)).isNotNull &&
    col(AverageOccupancyCol).cast("double") > col(mirrorCol(AverageOccupancyCol)).cast("double") - 0.5 &&
    col(AverageOccupancyCol).cast("double") < col(mirrorCol(AverageOccupancyCol)).cast("double") + 0.5

  private val primaryDomainMatches = col(PrimaryDomainCol).isNotNull &&
    col(mirrorCol(PrimaryDomainCol)).isNotNull &&
    col(PrimaryDomainCol) === col(mirrorCol(PrimaryDomainCol))

  private val capacityMatches = col(CapacityCol).isNotNull &&
    col(mirrorCol(CapacityCol)).isNotNull &&
    col(CapacityCol) === col(mirrorCol(CapacityCol))

  private val dateColMatches = (colName: String) => col(colName).isNotNull &&
    col(mirrorCol(colName)).isNotNull &&
    col(colName).cast("int") > col(mirrorCol(colName)).cast("int") - 5 &&
    col(colName).cast("int") < col(mirrorCol(colName)).cast("int") + 5


  def processData(spark: SparkSession, df: DataFrame): DataFrame = {
    def buildScores(df: DataFrame): DataFrame = {
      val mirrorColNames = buildMirrorSchema(df.columns)
      val mirrorDF = df.toDF(mirrorColNames: _*)
      val joinedDF = df.join(mirrorDF, baseJoinCondition)
      val bigScoresDF = joinedDF
        .withColumn(ScoreCol, when(bedroomsMatches, 10).otherwise(0))
        .withColumn(ScoreCol, when(bathroomsMatches, col(ScoreCol) + 10).otherwise(col(ScoreCol)))
        .withColumn(ScoreCol, when(geoHashMatches, col(ScoreCol) + 10).otherwise(col(ScoreCol)))
        .withColumn(ScoreCol, when(averageDailyRateMatches, col(ScoreCol) + 10).otherwise(col(ScoreCol)))
        .withColumn(ScoreCol, when(averageOccupancyMatches, col(ScoreCol) + 10).otherwise(col(ScoreCol)))
        .withColumn(ScoreCol, when(primaryDomainMatches, col(ScoreCol) + 10).otherwise(col(ScoreCol)))
        .withColumn(ScoreCol, when(capacityMatches, col(ScoreCol) + 10).otherwise(col(ScoreCol)))

      dateColumns(df.columns).foldLeft(bigScoresDF)(
        (tmpDF, colName) =>
          tmpDF.withColumn(ScoreCol, when(dateColMatches(colName), col(ScoreCol) + 1).otherwise(col(ScoreCol)))
      )
    }

    def findMatches(df: DataFrame, scoreDF: DataFrame): DataFrame = {
      import spark.implicits._
      val edgesDF = scoreDF.where(col(ScoreCol) >= 70).select(IdCol, mirrorCol(IdCol))
      val nodesRDD = df.rdd.map(r => (r.getAs[VertexId](IdCol), r.getAs[String](ListingIdCol)))
      val edgesRDD = edgesDF.rdd.map(r => Edge(r.getAs[VertexId](IdCol), r.getAs[VertexId](mirrorCol(IdCol)), null))
      val graph = Graph(nodesRDD, edgesRDD)
      val connectedComponents = graph.connectedComponents()
      val conCompVertices = connectedComponents.vertices.toDF
      conCompVertices
        .join(df, col(IdCol) === col("_1"))
        .groupBy("_2")
        .agg(collect_set(col(ListingIdCol)).as(GroupCol))
        .where(size(col(GroupCol)) > 1)
    }

    val mainDF = df.withColumn(IdCol, monotonically_increasing_id())
    val scoreDF = buildScores(mainDF)
    findMatches(mainDF, scoreDF)
  }
}
