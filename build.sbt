ThisBuild / name := "spark-streaming-playground"
ThisBuild / scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "com.typesafe" % "config" % "1.4.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "joda-time" % "joda-time" % "2.9.9",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyJarName := "spark-streaming-playground.jar"
assembly / fullClasspath := (Compile / fullClasspath).value
