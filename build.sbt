name := "kstreamex"

scalaVersion := "2.12.1"

version := "0.0.1"

val embedVersion = "0.15.1"
val kafkaVersion      = "0.11.0.0"
val scalaVersion1     = "2.12.1"
val scalatestVersion  = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" %  "kafka-clients"                    % kafkaVersion,
  "org.apache.kafka" %  "kafka-streams"                    % kafkaVersion,
  "org.scala-lang"   % "scala-library"                     % scalaVersion1,
  "net.manub" %% "scalatest-embedded-kafka" % embedVersion,
  "net.manub" %% "scalatest-embedded-kafka-streams" % embedVersion,
  "org.scalatest"    %% "scalatest"                        % scalatestVersion  % Test
)
