scalaVersion := "2.13.8"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"


libraryDependencies ++= Seq(
  "org.scala-lang.modules"  %% "scala-parser-combinators"       % "2.1.1",
  "ch.qos.logback"          %  "logback-classic"                % "1.2.9",
  "org.scalatest"           %% "scalatest"                      % "3.2.12" % Test,
)