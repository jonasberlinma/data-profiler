import AssemblyKeys._

name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
// Spark dependency
"org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
"org.apache.spark" %% "spark-graphx" % "2.4.0" % "provided",

// Third-party libraries
"net.sf.jopt-simple" % "jopt-simple" % "4.3",
"joda-time" % "joda-time" % "2.0",
"org.scalikejdbc" %% "scalikejdbc" % "2.5.0",
"com.h2database" % "h2" % "1.4.193",
"ch.qos.logback" % "logback-classic" % "1.1.7",
"org.postgresql" % "postgresql" % "9.4-1201-jdbc41"
)

assemblySettings
//scalikejdbcSettings


jarName in assembly := "my-project-assembly.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

