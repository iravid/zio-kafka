package zio.kafka.serde

import zio.{ RIO, Task }
import zio.blocking.{ Blocking, blocking => zioBlocking }
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{ Serializer => KafkaSerializer }

import scala.jdk.CollectionConverters._

/**
 * Serializer from values of some type T to a byte array
 *
 * @tparam R Environment available to the serializer
 * @tparam T
 */
trait Serializer[-R, -T] {
  def serialize(topic: String, headers: Headers, value: T): RIO[R, Array[Byte]]

  /**
   * Create a serializer for a type U based on the serializer for type T and a mapping function
   */
  def contramap[U](f: U => T): Serializer[R, U] =
    Serializer((topic, headers, u) => serialize(topic, headers, f(u)))

  /**
   * Create a serializer for a type U based on the serializer for type T and an effectful mapping function
   */
  def contramapM[R1 <: R, U](f: U => RIO[R1, T]): Serializer[R1, U] =
    Serializer((topic, headers, u) => f(u).flatMap(serialize(topic, headers, _)))

  /**
   * Returns a new serializer that executes its serialization function on the blocking threadpool.
   */
  def blocking: Serializer[R with Blocking, T] =
    Serializer((topic, headers, t) => zioBlocking(serialize(topic, headers, t)))

  /**
   * Returns a new serializer that handles optional values and serializes them as nulls.
   */
  def asOption[U <: T](implicit ev: Null <:< T): Serializer[R, Option[U]] =
    contramap(_.orNull)
}

object Serializer extends Serdes {

  /**
   * Create a serializer from a function
   */
  def apply[R, T](ser: (String, Headers, T) => RIO[R, Array[Byte]]): Serializer[R, T] =
    new Serializer[R, T] {
      override def serialize(topic: String, headers: Headers, value: T): RIO[R, Array[Byte]] =
        ser(topic, headers, value)
    }

  /**
   * Create a Serializer from a Kafka Serializer
   */
  def fromKafkaSerializer[T](
    serializer: KafkaSerializer[T],
    props: Map[String, AnyRef],
    isKey: Boolean
  ): Task[Serializer[Any, T]] =
    Task(serializer.configure(props.asJava, isKey))
      .as(
        new Serializer[Any, T] {
          override def serialize(topic: String, headers: Headers, value: T): Task[Array[Byte]] =
            Task(serializer.serialize(topic, headers, value))
        }
      )

}
