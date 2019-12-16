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
    val dateTimeRange = granularity.datesBetween(startDate, endDate)

    logger.info(s"Streaming ${dateTimeRange.length} datetimes with intervals in $granularity")

    Source(dateTimeRange.toList)
  }
}
