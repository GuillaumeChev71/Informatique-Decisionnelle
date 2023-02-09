ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

// https://mvnrepository.com/artifact/oracle/ojdbc6
libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.5.3",
  "org.oracle" % "ojdbc6" % "11.2.0.3" % "system"
)

lazy val root = (project in file("."))
  .settings(
    name := "Projet"
  )
