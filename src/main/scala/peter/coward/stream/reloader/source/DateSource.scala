package peter.coward.stream.reloader.source

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import org.joda.time.base.BaseSingleFieldPeriod
import org.joda.time._
import peter.coward.stream.reloader.utils.Granularity

class DateSource[A <: BaseSingleFieldPeriod](
  startDate: DateTime,
  endDate: DateTime,
  granularity: Granularity[A]
) extends StrictLogging {
  def source: Source[DateTime, NotUsed] = {

    //Granularity contains a function for working out the interval between two datetimes
    //Use this to generate a list of datetimes at the correct granularity
    val dateTimeRange = datesBetween(startDate, endDate, granularity.between)

    logger.info(s"Streaming ${dateTimeRange.length} datetimes with intervals in $granularity")

    Source(dateTimeRange.toList)
  }

  /**
    * Generate an IndexedSeq of all date times between a start and end datetime
    * @param start datetime to begin generating from
    * @param end datetime to end the list with
    * @param between function that calculates the interval between two datetimes at a given granularity
    * @return IndexedSeq of datetimes
    */
  private def datesBetween(start: DateTime, end: DateTime, between: (ReadableInstant, ReadableInstant) => A): IndexedSeq[DateTime] = {
    val interval = between(start, end)
    val intervalType = interval.getFieldType
    for {
      time <- 0 to interval.get(intervalType)
    } yield startDate.withFieldAdded(intervalType, time)
  }
}
