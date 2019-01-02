
name := "bz-parse-aggregate"

version := "1.0"

scalaVersion := "2.11.11"


val akkaVersion = "2.5.0"

resolvers += "Spring Plugins Repository" at "http://repo.spring.io/plugins-release/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.rogach" %% "scallop" % "3.1.5",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.18",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "com.github.svezfaz" %% "akka-stream-checkpoint-core" % "0.0.3",
  "com.github.svezfaz" %% "akka-stream-checkpoint-dropwizard" % "0.0.3",
  "org.yaml" % "snakeyaml" % "1.8",
  "com.google.code.externalsortinginjava" % "externalsortinginjava" % "0.2.5",

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

mainClass in assembly := Some("com.mkorneev.bz.EventSequencerApp")