package peter.coward.stream.reloader.sink

import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{FlatSpec, Matchers}
import peter.coward.stream.reloader.messages.Event

class KafkaSinkSpec extends FlatSpec with Matchers with EmbeddedKafka {

  implicit val deserialiser = new StringDeserializer

  "KafkaSink" should "publish events to kafka" in {
    withRunningKafka {
      val producerConf = ConfigFactory.load().getConfig("akka.kafka.producer")
      val brokerList = "localhost:6001"
      val topic = "test-topic"

      val kafkaSink = new KafkaSink(brokerList, topic, producerConf)

      val eventData = """{"a":1, "b": "hello"}"""
      val inEvent = Event(
        "testEvent",
        "1.0.0",
        eventData
      )

      Source(List(inEvent))
        .via(kafkaSink.flow)
        .to(kafkaSink.sink)

      val result = consumeFirstMessageFrom(topic)

      result shouldEqual eventData
    }
  }

}
