package peter.coward.stream.reloader.flow

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import peter.coward.stream.reloader.messages.Event

class EventLoader(
  prefix: String,
  eventTypes: Map[String, List[String]],
  partitionNames: Boolean,
  getEvents: String => List[String]
) extends StrictLogging {
  def flow: Flow[DateTime, Event, NotUsed] = Flow[DateTime] mapConcat { dt =>
    eventTypes flatMap {
      case (eventName, versions) =>
        versions flatMap { version =>
          val year = DateTimeFormat.forPattern("yyyy").print(dt)
          val month = DateTimeFormat.forPattern("MM").print(dt)
          val day = DateTimeFormat.forPattern("dd").print(dt)

          //TODO: Granularity
          val path = if (partitionNames) {
            s"$prefix/$eventName/$version/year=$year/month=$month/day=$day"
          } else {
            s"$prefix/$eventName/$version/$year/$month/$day"
          }

          val rawEvents = getEvents(path)

          logger.info(s"Retrieved ${rawEvents.length} events for $eventName/$version")

          rawEvents map { data =>
            Event(
              eventName,
              version,
              data
            )
          }
        }
    }
  }
}
