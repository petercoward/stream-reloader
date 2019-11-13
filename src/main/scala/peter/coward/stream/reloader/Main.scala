package peter.coward.stream.reloader

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.DateTime
import peter.coward.stream.reloader.config.Config
import peter.coward.stream.reloader.flow.EventLoader
import peter.coward.stream.reloader.messages.Granularity
import peter.coward.stream.reloader.sink.KafkaSink
import peter.coward.stream.reloader.source.DateSource
import peter.coward.stream.reloader.utils.S3Utils

object Main extends App with StrictLogging {
  logger.info("stream-reloader spooling up...")

  val config = Config(args)

  implicit val system = ActorSystem("stream-reloader")
  implicit val materializer = ActorMaterializer()

  //TODO: validate config, e.g. start date must be before end date

  val producerConfig = ConfigFactory.load().getConfig("akka.kafka.producer")

  val client = AmazonS3ClientBuilder
    .standard
    .withPathStyleAccessEnabled(true)
    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
    .build

  val s3 = new S3Utils(client, config.sourceBucket)

  val granularity = Granularity.getByName(config.granularity)
  //TODO: set up source class
  val source = new DateSource(
    config.startDate,
    config.endDate.getOrElse(DateTime.now),
    granularity
  ).source

  //TODO: set up flow(s)
  val eventLoader = new EventLoader(
    config.sourcePrefix,
    config.events,
    config.partitionNames,
    s3.getFilesInPath
  ).flow

  //TODO: Set up sink class
  val kafkaSink = new KafkaSink(
    config.brokerList,
    config.destinationTopic,
    producerConfig
  )

  //TODO: run graph
  source
    .via(eventLoader)
    .via(kafkaSink.flow)
    .runWith(kafkaSink.sink)
}
