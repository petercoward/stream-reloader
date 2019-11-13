package peter.coward.stream.reloader.sink

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Flow, Sink}
import akka.{Done, NotUsed}
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import peter.coward.stream.reloader.messages.Event

import scala.concurrent.Future

class KafkaSink(brokerList: String, topic: String, producerConfig: Config) {
  private val producerSettings =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
    .withBootstrapServers(brokerList)

  def flow: Flow[Event, ProducerRecord[String, String], NotUsed] = Flow[Event] map { event =>
    new ProducerRecord[String, String](topic, event.eventData)
  }

  def sink: Sink[ProducerRecord[String, String], Future[Done]] = Producer.plainSink(producerSettings)
}
