import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.0",
      version      := "0.1.0"
    )),
    name := "SparkTest",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0",


    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
      case "log4j.properties"                                  => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )
