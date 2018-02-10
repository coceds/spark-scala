name := "Scala-bigdata"

version := "0.1"

scalaVersion := "2.10.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.2.0",
  "mysql" % "mysql-connector-java" % "5.1.44"
)


