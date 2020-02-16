package zio.kafka.client

import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.test._
import zio.test.TestAspect.timeout
import zio.test.environment.TestEnvironment
import zio.kafka.client.embedded.Kafka
import zio.kafka.client.serde.Serializer
import zio.clock.Clock
import zio.duration._

object ConsumerBenchmark extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Throwable] =
    suite("Consumer benchmarks")(
      testM("Plain consumer") {
        for {
          bootstrapServers <- Kafka.bootstrapServers
          _ <- AdminClient.make(AdminClientSettings(bootstrapServers)).use { client =>
                client.createTopic(AdminClient.NewTopic("test-topic", 1, 1))
              }
          _ <- Producer.make(ProducerSettings(bootstrapServers), Serializer.string, Serializer.string).use { producer =>
                val record =
                  new ProducerRecord[String, String]("test-topic", 0, null, null, List.fill(1000)("a").mkString)
                ZIO.collectAll(ZIO.replicate(250000)(producer.produce(record))).flatMap(ZIO.foreach_(_)(identity))
              }
        } yield assertCompletes
      }
    ).provideSomeLayerShared[TestEnvironment](
      Kafka.embedded.mapError(TestFailure.fail) ++ Clock.live
    ) @@ timeout(180.seconds)
}
