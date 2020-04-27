name := "template2"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "1.7.6",
  "com.typesafe.play" %% "play-json" % "2.4.11",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
  "com.vividsolutions" % "jts-core" % "1.14.0",// See https://github.com/sksamuel/elastic4s/issues/507
  "com.typesafe.akka" %% "akka-stream" % "2.5.18",
  "com.typesafe.akka" %% "akka-distributed-data" % "2.5.18",
  "com.lightbend.akka" %% s"akka-stream-alpakka-elasticsearch" % "1.0-M1",
  "io.spray" %% "spray-json" % "1.3.4",
 "org.apache.commons" % "commons-compress" % "1.15"

)