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
  getDatePath: DateTime => String,
  getEvents: String => List[String]
) extends StrictLogging {
  def flow: Flow[DateTime, Event, NotUsed] = Flow[DateTime] mapConcat { dt =>
    eventTypes flatMap {
      case (eventName, versions) =>
        versions flatMap { version =>

          val datePath = getDatePath(dt)

          val fullPath = s"$prefix/$eventName/$version/$datePath"

          val rawEvents = getEvents(fullPath)

          logger.info(s"Retrieved ${rawEvents.length} events for $eventName/$version")

          rawEvents map { data =>
            Event(eventName, version, data)
          }
        }
    }
  }
}
