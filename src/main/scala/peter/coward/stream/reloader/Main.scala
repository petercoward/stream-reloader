package peter.coward.stream.reloader

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.DateTime
import peter.coward.stream.reloader.config.Config
import peter.coward.stream.reloader.flow.EventLoader
import peter.coward.stream.reloader.sink.KafkaSink
import peter.coward.stream.reloader.source.DateSource
import peter.coward.stream.reloader.utils.{FileUtils, Granularity}

object Main extends App with StrictLogging {
  //Set up actor system + materializer
  implicit val system = ActorSystem("stream-reloader")
  implicit val materializer = ActorMaterializer()

  logger.info("stream-reloader spooling up...")

  //Load the config from args passed
  val config = Config(args)

  //TODO: validate config, e.g. start date must be before end date

  //Load kafka producer config from application.conf
  val producerConfig = ConfigFactory.load().getConfig("akka.kafka.producer")

  //Based on the mode supplied in config, get the relevant fileUtils (s3 or local)
  //and then get the fileLoader function
  val fileUtils = FileUtils.getFileUtils(config)
  val fileLoaderFunc = fileUtils.getFileLoader(config.gzipped)

  //Get the granularity at which to load events based on what was passed in config
  val granularity = Granularity.getByName(config.granularity)

  //From Granularity, get the function that generates the date path to load from
  //Partially apply it with the partitionNames flag
  //When EventLoader uses this it will only need to pass it the datetime to generate a path from
  val datePathFunc = granularity.datePath(config.partitionNames, _)

  //Set up the source that will generate a stream of datetimes
  val source = new DateSource(
    config.startDate,
    config.endDate.getOrElse(DateTime.now),
    granularity
  ).source

  //Set up the flow that will load up the events
  val eventLoader = new EventLoader(
    config.sourcePrefix,
    config.events,
    datePathFunc,
    fileLoaderFunc
  ).flow

  //Set up the Kafka sink to write to kafka
  val kafkaSink = new KafkaSink(
    config.brokerList,
    config.destinationTopic,
    producerConfig
  )

  //Run the graph
  source
    .via(eventLoader)
    .via(kafkaSink.flow)
    .runWith(kafkaSink.sink)

}
