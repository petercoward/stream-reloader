import sbt._

object Dependencies {
  val scalaTestV = "3.0.8"
  
  lazy val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % scalaTestV % Test
  )
  
  val akkaV = "2.5.25"
  
  lazy val akka = Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test
  )
  
  val alpakkaV = "1.0.5"
  
  lazy val alpakka = Seq(
    "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaV
  )
  
  val embeddedKafkaV = "2.3.1"
  
  lazy val embeddedKafka = Seq(
    "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaV % Test
  )
  
  val awscalaV = "0.8.+"

  lazy val awscala = Seq(
    "com.github.seratch" %% "awscala-s3" % awscalaV
  )
  
  lazy val s3 = Seq(
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.623",
    "io.findify" %% "s3mock" % "0.2.4" % Test
  )
  
  val jodaV = "2.10.3"
  
  lazy val joda = Seq(
    "joda-time" % "joda-time" % jodaV
  )
  
  val scoptV = "4.0.0-RC2"
  
  lazy val scopt = Seq(
    "com.github.scopt" %% "scopt" % scoptV
  )
  
  lazy val logging = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val deps = scalaTest ++ akka ++ alpakka ++ embeddedKafka ++ s3 ++ joda ++ scopt ++ logging
}
