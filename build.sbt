
name := "bz-parse-aggregate"

version := "1.0"

scalaVersion := "2.11.11"


val akkaVersion = "2.5.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  //test scope
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)


assemblyMergeStrategy in assembly := {
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in assembly := Some("com.manonthegithub.bz.CsvConverterApp")