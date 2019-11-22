package peter.coward.stream.reloader.config

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scopt.{OParser, Read}

case class Config(startDate: DateTime = DateTime.now(),
                  endDate: Option[DateTime] = None,
                  granularity: String = "",
                  partitionNames: Boolean = false,
                  mode: String = "local",
                  gzipped: Boolean = false,
                  events: Map[String, List[String]] = Map(),
                  sourceBucket: String = "",
                  sourcePrefix: String = "",
                  destinationTopic: String = "",
                  brokerList: String = "")

object Config {
  implicit val dateTimeRead: Read[DateTime] = new Read[DateTime] {
    override def arity: Int = 1

    override def reads: String => DateTime = { s: String =>
      val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")
      formatter.parseDateTime(s)
    }
  }

  implicit val eventsMapRead: Read[Map[String, List[String]]] = new Read[Map[String, List[String]]] {
    override def arity: Int = 1

    override def reads: String => Map[String, List[String]] = { arg: String =>
      arg.split(",") map { s =>
        val splitList = s.split("=").toList
        val versions = splitList.tail flatMap { _.split('|') }

        (splitList.head, versions)
      } toMap
    }
  }

  private val granularities = List(
    "years",
    "months",
    "days",
    "hours"
  )

  private val modes = List(
    "local",
    "s3"
  )

  def apply(args: Array[String]): Config = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._

      OParser.sequence(
        programName("stream-reloader"),
        head("stream-reloader", "1.0.0"),
        opt[DateTime]("startDate")
          .required()
          .valueName("<dd/MM/yyyy>")
          .action((x, c) => c.copy(startDate = x))
          .text("Date (format dd/MM/yyyy) from which to start reloading. Required property."),
        opt[DateTime]("endDate")
          .optional()
          .valueName("<dd/MM/yyyy>")
          .action((x, c) => c.copy(endDate = Some(x)))
          .text("Date (format dd/MM/yyyy) of the end of the period to start reloading. Optional property, if not provided will default to today's date"),
        opt[String]("granularity")
          .required()
          .validate(x =>
            if (granularities.contains(x)) success
            else failure("Granularity must be one of the following: years, months, days, hours")
          )
          .action((x, c) => c.copy(granularity = x))
          .text("The granularity of the folder structure to load from. Can be 'years', 'months', 'days' or 'hours'."),
        opt[Boolean]("partitionNames")
          .optional()
          .action((x, c) => c.copy(partitionNames = x))
          .valueName("true/false")
          .text("Indicates whether the source folders have date partition names i.e. /year=yyyy/month=MM/day=dd/"),
        opt[String]("mode")
            .optional()
            .validate(x =>
              if (modes.contains(x)) success
              else failure("Mode must be one of the following: 's3', 'local'")
            )
            .action((x, c) => c.copy(mode = x))
            .valueName("s3/local")
            .text("The mode to run stream-reloader in. Options are: 's3', 'local'. Default value is 'local'"),
        opt[Boolean]("gzipped")
            .optional()
            .action((x, c) => c.copy(gzipped = x))
            .valueName("true/false")
            .text("Indicates whether the files to reload are gzipped"),
        opt[Map[String, List[String]]]("events")
          .required()
          .valueName("event1=v1|v2,event2=v2|v3|v4...")
          .action((x, c) => c.copy(events = x))
          .text("Events and their schema versions to be reloaded. Must be in the format 'event1=v1|v2,event2=v2|v3|v4' etc"),
        opt[String]("sourceBucket")
          .optional()
          .action((x, c) => c.copy(sourceBucket = x))
          .text("The s3 bucket from which to reload events."),
        opt[String]("sourcePrefix")
            .required()
            .action((x, c) => c.copy(sourcePrefix = x))
            .text("The prefix this event is stored under"),
        opt[String]("destination")
          .required()
          .action((x, c) => c.copy(destinationTopic = x))
          .text("The topic name to reload events onto"),
        opt[String]("brokerList")
          .required()
          .action((x, c) => c.copy(brokerList = x))
          .text("The list of IPs of the kafka brokers to write to")
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) => config
      case _ => throw new IllegalArgumentException("Invalid Arguments")
    }
  }
}
