name := "sparkscalaproject"

version := "0.1"

scalaVersion := "2.11.12"
resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
// https://mvnrepository.com/artifact/edu.ucar/netcdf
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "edu.ucar" % "cdm" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "grib" % "4.5.5" exclude("commons-logging", "commons-logging"),
  "edu.ucar" % "netcdf4" % "4.5.5" exclude("commons-logging", "commons-logging")
)
