package peter.coward.stream.reloader.source

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.base.BaseSingleFieldPeriod
import org.joda.time._
import peter.coward.stream.reloader.messages.Granularity

class DateSource[A <: BaseSingleFieldPeriod](
  startDate: DateTime,
  endDate: DateTime,
  //granularity: String,
  granularity: Granularity[A]
) extends StrictLogging {
  def source: Source[DateTime, NotUsed] = {

    val dateTimeRange = datesBetween(startDate, endDate, granularity.between)

    logger.info(s"Streaming ${dateTimeRange.length} datetimes with intervals in $granularity")

    Source(dateTimeRange.toList)
  }

  private def datesBetween(
    start: DateTime,
    end: DateTime,
    between: (ReadableInstant, ReadableInstant) => A
  ): IndexedSeq[DateTime] = {
    val interval = between(start, end)
    val intervalType = interval.getFieldType
    for {
      time <- 0 to interval.get(intervalType)
    } yield startDate.withFieldAdded(intervalType, time)
  }
}
