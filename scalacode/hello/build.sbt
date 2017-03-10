import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.0",
      version      := "0.1.0"
    )),
    name := "Hello",
    libraryDependencies += scalaTest % Test
  )
