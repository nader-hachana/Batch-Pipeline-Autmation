lazy val root = (project in file("."))
  .settings(
    version := "0.1",
    scalaVersion := "2.12.17",
    organization := "com.cognira.checking",
    name := "Data Quality Module",
    sbtVersion := "1.2.8"
  )

assemblyJarName in assembly := s"checking_data_quality_2.12-0.1.jar"

logLevel in assembly := Level.Error

libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
      "org.apache.logging.log4j" % "log4j-api" % "2.17.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.17.1",
      "com.datastax.spark" %% "spark-cassandra-connector-assembly" % "3.2.0",
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.11.0"
    )
    
scalacOptions += "-deprecation"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}