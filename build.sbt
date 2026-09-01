name := "spark-etl-utils"
version := "0.6.0"
scalaVersion := "2.13.12"
organization := "dev.mustafakaplan"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.17" % "test"
)
