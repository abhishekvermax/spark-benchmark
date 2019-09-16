name := "spark-benchmark"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.6.0",
  "org.json4s" %% "json4s-native" % "3.3.0",
  "com.novocode" % "junit-interface" % "latest.release" % Test,
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "org.scalanlp" %% "breeze" % "0.13.2", //% "provided",
  "com.squants"  %% "squants"  % "0.6.2"
)