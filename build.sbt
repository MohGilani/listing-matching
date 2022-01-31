ThisBuild / name := "listing-matching"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.mhsg.listing-matching"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

val sparkVersion = "2.4.8"
val mainDependencies = Seq(
  "org.apache.spark"  %%  "spark-core"    % sparkVersion,
  "org.apache.spark"  %%  "spark-sql"     % sparkVersion,
  "org.apache.spark"  %%  "spark-graphx"  % sparkVersion,
  "ch.hsr"            %   "geohash"       % "1.4.0",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "net.ruippeixotog" %% "scala-scraper" % "2.2.1"
)

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= mainDependencies,
    mainClass / run := Some("com.mhsg.hopper.ListingMatching")
  )
